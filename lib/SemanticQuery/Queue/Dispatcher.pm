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
use Coro;
use Coro::Socket;
use Data::Dumper qw(Dumper);
use Carp qw(croak);
use Storable qw(freeze thaw);
use JSON;
use Digest::MD5 qw(md5_hex);
use ZMQ::LibZMQ3;
use ZMQ::Constants qw(ZMQ_PUSH ZMQ_PULL ZMQ_SNDMORE);
use SemanticQuery::Logger;
use SemanticQuery::Data::URL;
use SemanticQuery::Data::Query;

use constant MAX_MSGLEN => 1024;
use constant CHANNEL_QUEUE => 512;

my ($context, $publisher, $socket);
my $s_interrupted = 0;
my $SOCKETS = new Coro::Channel CHANNEL_QUEUE;
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
	croak("Couldn't bind to socket") unless (defined($socket));
}

sub loop {
	my $self = shift;
	my @threads;
	log_info { 'Beginning main Dispatcher loop' };

	push(@threads,new Coro \&_collector, $self->ZMQ_PULLPOINT);
	$threads[-1]->ready;

	while (!$s_interrupted) {
		my $remote = $socket->accept;
		log_debug { 'accepting socket' };
		push(@threads,new Coro \&_dispatch, $remote, $self->ZMQ_PUSHPOINT);
		$threads[-1]->ready;
	}

	$_->join for (@threads);
	croak("Caught interrupt");
}

sub _dispatch {
	my $remote = shift;
	my $endpoint = shift;
	croak('Undefined socket') unless (defined($remote));
	my $task = "";

	while (<$remote>) {
		$task .= $_;
	}

	my $req_id = _do_zmq_request($task, $endpoint);
	sleep 1;
	cede;

	while (1) {
		my $buf = $SOCKETS->get();
		log_debug { Dumper($buf) };
		next unless defined($buf);
		if (substr($buf,1,length($req_id)) eq $req_id) {
			my $resp_obj = thaw(substr($buf,length($req_id)));
			#XXX unwrap object
			$remote->send(Dumper($resp_obj));
			last;
		} else {
			# whoops
			log_debug { "Placing mismatched request back into the queue" };
			$SOCKETS->put($buf);	
			sleep 1; #just to be safe
		}
	}

	return;
}

sub _do_zmq_request {
	my $task = shift;
	my $endpoint = shift;
	my $ctx = zmq_init();
	my $pub = zmq_socket($ctx, ZMQ_PUSH);
	my $res = zmq_bind($pub, $endpoint);
	croak("Couldn't connect to ZMQ endpoint " . $endpoint . ", got error " . zmq_strerror(zmq_errno)) unless ($res == 0);

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
	$SOCKETS->put($req_id);

	return $req_id;
}

sub _collector {
	my $endpoint = shift;
	my $ctx = zmq_init();
	my $sub = zmq_socket($ctx, ZMQ_PULL);
	my $res = zmq_connect($sub, $endpoint);
	my $obj_blob = "0";
	my $resp_id = "0";
	my @ids;
	while (1) {
		push @ids,$_ while (defined($_ = $SOCKETS->get()));
		log_debug { "ids contains: " . join(", ",@ids) };
		$resp_id = s_recv($sub);
		next if ($resp_id eq "0");
		$obj_blob = s_recv($sub) while ($obj_blob eq "0");
		unless ($resp_id ~~ @ids) {
			# reject the message
			log_warn { "Dropped message with mismatched response ID $resp_id" };
			next;
		}
		# drop the id from the array XXX
		$SOCKETS->put('_'.$resp_id.$obj_blob);
		cede;
	} 
}

sub s_recv {
	my $sock = shift;
	my $buf;
	my $size = zmq_recv($sock, $buf, MAX_MSGLEN);
	return "0" if ($size > 0 || !(defined($buf)));
	return substr($buf, 0, $size);
}

1;
