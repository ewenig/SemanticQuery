package SemanticQuery::Queue::Dispatcher;

use 5.010;
use Mouse;
use Mouse::Util::TypeConstraints;

#worker types
enum 'SocketType' => qw(UNIX TCP);

# attributes
has 'SOCK_TYPE'     => (is => 'ro', isa => 'SocketType', required => 1);
has 'ZMQ_PUSHPOINT' => (is => 'ro', isa => 'Str', required => 1);
has 'ZMQ_PULLPOINT' => (is => 'ro', isa => 'Str', required => 0, default => 'tcp://localhost:7071');
has 'LOCAL_PORT'    => (is => 'ro', isa => 'Int', required => 0, default => 5000);
has 'SOCK_FILE'     => (is => 'ro', isa => 'Str', required => 0, default => '/tmp/textwise.sock');
has 'QUEUE_SIZE'    => (is => 'ro', isa => 'Int', required => 0, default => 10);

use strict;
use warnings;
use Errno qw(EAGAIN);
use IO::Select;
use Coro;
use Coro::Socket;
use Coro::Debug;
use Data::Dumper qw(Dumper);
use Carp qw(croak);
use Storable qw(freeze thaw);
use JSON;
use Digest::MD5 qw(md5_hex);
use ZMQ::LibZMQ3;
use ZMQ::Constants qw(ZMQ_PUSH ZMQ_PULL ZMQ_SNDMORE ZMQ_DONTWAIT);
use SemanticQuery::Logger;
use SemanticQuery::Data::URL;
use SemanticQuery::Data::Query;

use constant MAX_MSGLEN => 1024;
use constant CHANNEL_QUEUE => 512;

my ($context, $publisher, $socket);
my $s_interrupted = 0;
my $SOCKETS = {};
my $LOCK = new Coro::Semaphore;
$SIG{'INT'} = \&_handler;

sub _handler {
	$s_interrupted = 1;
}

sub BUILD {
	my $self = shift;

	# Receiver socket initialization
	if ($self->SOCK_TYPE eq 'TCP') {
		$socket = new Coro::Socket( LocalHost => '127.0.0.1', LocalPort => $self->LOCAL_PORT, Proto => 'tcp', Listen => $self->QUEUE_SIZE, ReuseAddr => 1)
	}
	if ($self->SOCK_TYPE eq 'UNIX') {
		use IO::Socket::UNIX;
		$socket = Coro::Socket->new_from_fh(new IO::Socket::UNIX( Type => SOCK_STREAM, Local => $self->SOCK_FILE, Listen => $self->QUEUE_SIZE ));
	}
	die("Couldn't bind to socket") unless (defined($socket));
}

sub loop {
	my $self = shift;
	my @threads;
	log_info { 'Beginning main Dispatcher loop' };

	push(@threads, new Coro \&_collector, $self->ZMQ_PULLPOINT);
	$threads[-1]->ready;

	while (!$s_interrupted) {
		$socket->accept;
		log_debug { 'accepting socket' };
		push(@threads,new Coro \&_dispatch, $_->accept(), $self->ZMQ_PUSHPOINT);
		$threads[-1]->ready;
		#cede;
		#$remote->close;
	}

	$_->join for (@threads);
	croak("Caught interrupt");
}

sub _dispatch {
	my $remote = shift;
	my $endpoint = shift;
	die('Undefined socket') unless (defined($remote));
	my $task = "";

	while (<$remote>) {
		$task .= $_;
	}

	my $req_id = _do_zmq_request($task, $endpoint);

	while (1) {
		$LOCK->down;
		last if (defined($SOCKETS->{$req_id}) && $SOCKETS->{$req_id} ne '');
		$LOCK->up;
		cede;
	}
	my $resp_obj = thaw($SOCKETS->{$req_id});
	undef($SOCKETS->{$req_id});
	$LOCK->up;
	#XXX unwrap object
	$remote->send(Dumper($resp_obj));
	$remote->close;

	return;
}

sub _do_zmq_request {
	my $task = shift;
	my $endpoint = shift;
	my $ctx = zmq_init();
	my $pub = zmq_socket($ctx, ZMQ_PUSH);
	log_debug { "Binding push socket to ZMQ endpoint $endpoint" };
	my $res = zmq_bind($pub, $endpoint);
	die("Couldn't bind to ZMQ endpoint " . $endpoint . ", got error " . zmq_strerror(zmq_errno)) unless ($res == 0);

	# Task differentiation
	my $obj;
	my $json = from_json($task); # this call will die() on error

	my $type = $json->{'request'} or die();
	my $params = $json->{'parameters'} or die();

	if ($type eq 'Query') {
		$obj = SemanticQuery::Data::Query->new($params) or die(); # XXX
	} elsif ($type eq 'URL') {
		$obj = SemanticQuery::Data::URL->new($params) or die();
	} else {
		die('Task not recognized');
	}

	log_debug { Dumper($obj) };

	my $buf = freeze($obj);
	my $req_id = md5_hex($buf);
	do {
		zmq_send($pub,ref($obj),ZMQ_SNDMORE); # type hint
		zmq_send($pub,$buf,0);
	};

	# send request ID to sockets
	$LOCK->down;
	$SOCKETS->{$req_id} = '';
	$LOCK->up;

	return $req_id;
}

sub _collector {
	our $server = new_unix_server Coro::Debug "/tmp/socketpath";

	my $endpoint = shift;
	my $ctx = zmq_init();
	my $sub = zmq_socket($ctx, ZMQ_PULL);
	log_debug { "Connecting pull socket to ZMQ endpoint $endpoint" };
	my $res = zmq_connect($sub, $endpoint);
	die("Couldn't connect to ZMQ endpoint " . $endpoint . ", got error " . zmq_strerror(zmq_errno)) unless ($res == 0);
	my $obj_blob = "0";
	my $resp_id = "0";
	while (1) {
		$resp_id = s_recv($sub);
		if ($resp_id eq "0") {
			cede;
			next;
		}
		while ($obj_blob eq "0") {
			cede;
			$obj_blob = s_recv($sub);
		}
		$LOCK->down;
		unless (defined($SOCKETS->{$resp_id})) {
			# reject the message
			log_warn { "Dropped message with mismatched response ID $resp_id" };
			next;
		}
		$SOCKETS->{$resp_id} = $obj_blob;
		$LOCK->up;
	}

}

sub s_recv {
	my $sock = shift;
	my $buf;
	my $size = zmq_recv($sock, $buf, MAX_MSGLEN, ZMQ_DONTWAIT);
	return "0" if (zmq_errno == EAGAIN); #$size > 0 || !(defined($buf)));
	return substr($buf, 0, $size);
}

1;

=pod

=head1 SemanticQuery::Queue::Dispatcher

Dispatcher takes requests in JSON format via a UNIX or TCP socket. It uses
ZeroMQ to pass requests (in the form of messages) to the Workers (assuming one
is available).

Dispatcher takes JSON messages in this format:

=begin text
  {
    "request": request-type,
    "parameters":
      {
        params
      }
  }
=end text

where C<request-type> is one of C<URL> or C<Query> (corresponding to the two types
of requests) and C<params> is a JSON object representing the input parameters for
the given request.

=cut
