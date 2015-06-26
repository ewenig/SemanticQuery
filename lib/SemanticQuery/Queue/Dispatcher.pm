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

# Events & threads
use EV;
use AnyEvent;
use AnyEvent::Strict;
use AnyEvent::Socket;
use AnyEvent::Semaphore;
use Coro;

# Miscellaneous functions
use Data::Dumper qw(Dumper);
use Carp qw(croak);
use Storable qw(freeze thaw);
use JSON;
use Digest::MD5 qw(md5_hex);

# ZeroMQ libraries
use ZMQ::LibZMQ3;
use ZMQ::Constants qw(ZMQ_PUSH ZMQ_PULL ZMQ_SNDMORE ZMQ_DONTWAIT ZMQ_FD);

# Logging facility
use SemanticQuery::Logger;

# Data structures
use SemanticQuery::Data::URL;
use SemanticQuery::Data::Query;

# Constants
use constant MAX_MSGLEN => 1024;
use constant CHANNEL_QUEUE => 512;

my ($context, $publisher, $socket);
my $SOCKETS = {};
my $LOCK = AnyEvent::Semaphore->new(1);
$SIG{'INT'} = \&_handler;

sub _handler {
    EV::unloop;
}

sub loop {
	my $self = shift;
	log_info { 'Beginning main Dispatcher loop' };

	# Receiver socket config
    my ($interface, $port);
	if ($self->SOCK_TYPE eq 'TCP') {
		$interface = '127.0.0.1';
		$port = $self->LOCAL_PORT;
	}
	if ($self->SOCK_TYPE eq 'UNIX') {
		$interface = 'unix/';
		$port = $self->SOCK_FILE;
	}

	# init dispatcher event
    tcp_server($interface, $port, sub {
		my $remote = shift;
		die('Undefined socket') unless (defined($remote));
		my $task = "";

		while (<$remote>) {
		    $task .= $_;
		}

		my $req_id = _do_zmq_request($task, $self->ZMQ_PUSHPOINT);

		my $block = AnyEvent->condvar;
		my ($w, $resp_obj);
		while (1) {
			$w = $LOCK->down(sub { $block->send; });
			$block->recv; # block on the semaphore
			if (defined($SOCKETS->{$req_id}) && $SOCKETS->{$req_id} ne '') {
				$resp_obj = thaw($SOCKETS->{$req_id});
				undef $SOCKETS->{$req_id};
				last;
			}
			undef $w;
		}

		#XXX unwrap object
		$remote->send(Dumper($resp_obj));
		$remote->close;

		return;
    });

	# init collector event
	my $ctx = zmq_init();
	my $sub = zmq_socket($ctx, ZMQ_PULL);
	log_debug { "Connecting pull socket to ZMQ endpoint $self->ZMQ_PULLPOINT" };
	my $res = zmq_connect($sub, $self->ZMQ_PULLPOINT);
	die("Couldn't connect to ZMQ endpoint " . $self->ZMQ_PULLPOINT . ", got error " . zmq_strerror(zmq_errno)) unless ($res == 0);
	my $zmq_fh = zmq_getsockopt($sub, ZMQ_FD);
    my $zmq_loop = AnyEvent->io(
		fh   => $zmq_fh,
		poll => "r",
		cb   => sub {
		    my $obj_blob = "0";
		    my $resp_id = "0";
		    my $block = AnyEvent->condvar;
			$resp_id = s_recv($sub);
			if ($resp_id eq "0") {
				return;
			}
			while ($obj_blob eq "0") {
				$obj_blob = s_recv($sub);
			}
			my $w = $LOCK->down(sub {
			    if (defined($SOCKETS->{$resp_id})) {
			        $SOCKETS->{$resp_id} = $obj_blob;
			    } else {
   		 			# reject the message
		    		log_warn { "Dropped message with mismatched response ID $resp_id" };
		    	}
			    $block->send;
			});
			$block->recv;
			undef $w;
		}
    );

	# start main loop
	EV::loop;
	croak("Caught interrupt");
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
	my $block = AnyEvent->condvar;
	my $w = $LOCK->down(sub {
		$SOCKETS->{$req_id} = '';
		$block->send;
    });
	$block->recv;
	undef $w;

	return $req_id;
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
