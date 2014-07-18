package TextWise::Queue::Dispatcher;

use 5.010;
use Mouse;
use Mouse::Util::TypeConstraints;

#worker types
enum 'SocketType' => qw(UNIX TCP);

# attributes
has 'SOCK_TYPE'    => (is => 'ro', isa => 'SocketType', required => 1);
has 'ZMQ_ENDPOINT' => (is => 'ro', isa => 'Str', required => 1);
has 'LOCAL_PORT'   => (is => 'ro', isa => 'Int', required => 0, default => 5000);
has 'SOCK_FILE'    => (is => 'ro', isa => 'Str', required => 0, default => '/tmp/textwise.sock');
has 'QUEUE_SIZE'   => (is => 'ro', isa => 'Int', required => 0, default => 10);

use strict;
use warnings;
use Data::Dumper qw(Dumper);
use Carp qw(croak);
use Storable qw(freeze thaw);
use JSON;
use Digest::MD5 qw(md5_hex);
use ZMQ::LibZMQ3;
use ZMQ::Constants qw(ZMQ_PUB ZMQ_SNDMORE);
use TextWise::Logger;
use TextWise::Data::URL;
use TextWise::Data::Query;

use constant MAX_MSGLEN => 1024;

my ($context, $publisher, $socket);
my $s_interrupted = 0;
$SIG{'INT'} = \&_handler;

sub _handler {
	$s_interrupted = 1;
}

sub BUILD {
	my $self = shift;

	# ZeroMQ initialization
	$context = zmq_init();
	$publisher = zmq_socket($context, ZMQ_PUB);
	zmq_connect($publisher,$self->ZMQ_ENDPOINT);

	# Receiver socket initialization
	if ($self->SOCK_TYPE eq 'TCP') {
		use IO::Socket::INET;
		$socket = new IO::Socket::INET( LocalHost => '127.0.0.1', LocalPort => $self->LOCAL_PORT, Proto => 'tcp', Listen => $self->QUEUE_SIZE, ReuseAddr => 1)
	}
	if ($self->SOCK_TYPE eq 'UNIX') {
		use IO::Socket::UNIX;
		$socket = new IO::Socket::UNIX( Type => SOCK_STREAM, Local => $self->SOCK_FILE, Listen => $self->QUEUE_SIZE );
	}
	croak("Couldn't bind to socket") unless (defined($socket));
}

sub loop {
	my $self = shift;
	log_debug { 'Beginning main loop' };

	while (!$s_interrupted) {
		my $remote = $socket->accept;
		fork and next;
		_dispatch($remote);
	}

	croak("Caught interrupt");
}

sub _dispatch {
	my $remote = shift;
	croak('Undefined socket') unless (defined($remote));
	my $task = "";

	while (<$remote>) {
		$task .= $_;
	}

	my $resp = _do_zmq_request($task);
	log_debug { Dumper($resp) };
	exit(0);
}

sub _do_zmq_request {
	my $task = shift;

	# Task differentiation
	my $obj;
	my $json = from_json($task); # this call will die() on error

	my $type = $json->{'request'} or die();
	my $params = $json->{'parameters'} or die();

	if ($type eq 'Query') {
		$obj = TextWise::Data::Query->new($params) or die(); # XXX
	} elsif ($type eq 'URL') {
		$obj = TextWise::Data::URL->new($params) or die();
	} else {
		die('Task not recognized');
	}

	log_debug { Dumper($obj) };

	my $buf = freeze($obj);
	my $req_id = md5_hex($buf);
	zmq_send($publisher,ref($obj),ZMQ_SNDMORE); # type hint
	zmq_send($publisher,$buf,0);

	my ($obj_blob,$resp_id);
	do {
		$resp_id = s_recv($publisher);
		next unless(defined($resp_id));
		$obj_blob = s_recv($publisher);
		unless ($req_id == $resp_id) {
			log_warn { "Dropped message with mismatched response ID $resp_id" };
			# xxx reject the message
		}
	} while ($req_id != $resp_id);

	my $resp_obj = thaw($obj_blob);
	return $resp_obj;
}

sub s_recv {
	my $sock = shift;
	my $buf;
	my $size = zmq_recv($sock, $buf, MAX_MSGLEN);
	return undef if ($size > 0 || !(defined($buf)));
	return substr($buf, 0, $size);
}

1;
