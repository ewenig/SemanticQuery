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
use ZMQ::LibZMQ3;
use ZMQ::Constants qw(ZMQ_SUB ZMQ_SUBSCRIBE);
use TextWise::Data::URL;
use TextWise::Data::Query;

my ($context, $subscriber);
my $s_interrupted = 0;
$SIG{'INT'} = \&handler;

sub handler {
	$s_interrupted = 1;
}

sub BUILD {
	my $self = shift;
	$context = zmq_init();
	my $subscriber = zmq_socket($context, ZMQ_SUB);
	zmq_connect($subscriber,$self->ZMQ_ENDPOINT);
}

sub loop {
	my $self = shift;

	while (!$s_interrupted) {
		# do labor
		
	}

	# interrupt caught
	croak("Caught interrupt");
}

sub process_msg {

}

1;
