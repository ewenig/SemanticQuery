package TextWise::Queue::Worker;

use 5.010;
use Mouse;
use Mouse::Util::TypeConstraints;

#worker types
enum 'WorkerType' => qw(URL Query Both);

#attributes
has 'API_TOKEN'    => (is => 'ro', isa => 'Str');
has 'MONGO_HOST'   => (is => 'ro', isa => 'Str');
has 'MONGO_USER'   => (is => 'ro', isa => 'Str', required => 0);
has 'MONGO_PASS'   => (is => 'ro', isa => 'Str', required => 0);
has 'ZMQ_ENDPOINT' => (is => 'ro', isa => 'Str');
has 'WORKER_TYPE'  => (is => 'rw', isa => 'WorkerType');

use strict;
use warnings;
use Carp;
use Storable qw(thaw);
use Log::Contextual::SimpleLogger;
use Log::Contextual qw( :log ),
	-logger => Log::Contextual::SimpleLogger->new({
		levels_upto => 'debug',
		coderef => sub { print @_ },
	});
use ZMQ::LibZMQ3;
use ZMQ::Constants qw(ZMQ_SUB ZMQ_SUBSCRIBE);
use TextWise::Data::URL;
use TextWise::Data::Query;

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
		unless (defined($envelope)) {
			next;
		}
		my $obj_blob = s_recv($subscriber);
		my $obj = thaw($obj_blob);
		_process_msg($obj);
	}

	# interrupt caught
	croak("Caught interrupt");
}

sub _process_msg {
	my $object = shift;
	log_debug { Dumper($object->process) };
}

sub s_recv {
	# FIX THIS SECTION
	my $sock = shift;
	my $buf;
	my $size = 5;# = zmq_recv($sock, $buf, MAX_MSGLEN);
	return undef if ($size > 0);
	return substr($buf, 0, $size);
}

1;
