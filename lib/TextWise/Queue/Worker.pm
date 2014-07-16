package TextWise::Queue::Worker;

use 5.010;
use Mouse;
use Mouse::Util::TypeConstraints;

#worker types
enum 'WorkerType' => qw(URL Query Both);

#attributes
has 'API_TOKEN'    => (is => 'ro', isa => 'Str', required => 1);
has 'MONGO_HOST'   => (is => 'ro', isa => 'Str', required => 1);
has 'MONGO_USER'   => (is => 'ro', isa => 'Str', required => 0);
has 'MONGO_PASS'   => (is => 'ro', isa => 'Str', required => 0);
has 'ZMQ_ENDPOINT' => (is => 'ro', isa => 'Str', required => 1);
has 'WORKER_TYPE'  => (is => 'rw', isa => 'WorkerType', required => 1);

use strict;
use warnings;
use Carp;
use Storable qw(freeze thaw);
use Log::Contextual::SimpleLogger;
use ZMQ::LibZMQ3;
use ZMQ::Constants qw(ZMQ_SUB ZMQ_SUBSCRIBE ZMQ_SNDMORE);
use TextWise::Logger;
use TextWise::Data::URL;
use TextWise::Data::Query;
use TextWise::Data::Error;

use constant MAX_MSGLEN => 1024;

my ($context, $subscriber, $buf);
my $s_interrupted = 0;
$SIG{'INT'} = \&_handler;

sub _handler {
	$s_interrupted = 1;
}

sub BUILD {
	my $self = shift;
	$context = zmq_init();
	$subscriber = zmq_socket($context, ZMQ_SUB);
	zmq_connect($subscriber,$self->ZMQ_ENDPOINT);
	#filter out messages using type hints
	zmq_setsockopt($subscriber, ZMQ_SUBSCRIBE, 'TextWise::Data::Query') if ($self->WORKER_TYPE eq 'Query' || $self->WORKER_TYPE eq 'Both');
	zmq_setsockopt($subscriber, ZMQ_SUBSCRIBE, 'TextWise::Data::URL') if ($self->WORKER_TYPE eq 'URL' || $self->WORKER_TYPE eq 'Both');
}

sub loop {
	my $self = shift;

	while (!$s_interrupted) {
		my $envelope = s_recv($subscriber);
		next unless (defined($envelope));
		my $req_id = s_recv($subscriber);
		next unless (defined($req_id));
		my $obj_blob = s_recv($subscriber);

		# error handling
		local $SIG{__WARN__} = sub {
			my $obj = new TextWise::Data::Error(ERR_MESSAGE => shift);
			my $obj_blob = freeze($obj);
			zmq_send($subscriber,$req_id,ZMQ_SNDMORE);
			zmq_send($subscriber,$obj_blob,0);
		};

		my $obj = thaw($obj_blob);
		my $resp = _process_msg($obj);
		my $resp_blob = freeze($resp);
		zmq_send($subscriber,$req_id,ZMQ_SNDMORE);
		zmq_send($subscriber,$resp_blob,0);
	}

	# interrupt caught
	croak("Caught interrupt");
}

sub _process_msg {
	my $object = shift;
	my $result = $object->process;
	log_debug { Dumper($result) };
	return $result;
}

sub s_recv {
	my $sock = shift;
	my $buf;
	my $size = zmq_recv($sock, $buf, MAX_MSGLEN);
	return undef if ($size > 0 || !(defined($buf)));
	return substr($buf, 0, $size);
}

1;
