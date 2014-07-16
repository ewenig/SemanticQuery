package TextWise::Data::Query;

use 5.010;
use Mouse;

# attributes
has 'TEXT'      => (is => 'ro', isa => 'Str', required => 1);
has 'API_TOKEN' => (is => 'ro', isa => 'Str', required => 0);

use strict;
use warnings;
use TextWise::Logger;

sub process {
	# lazy package loading
	require TextWise::API::Category;
	require TextWise::API::Concept;

	my $self = shift;
	unless (defined($self->API_TOKEN)) {
		carp(__PACKAGE__ . ' requires API token');
		return;
	}

	my $categorizer = TextWise::API::Category->new(API_TOKEN => $self->API_TOKEN);
	my $conceiver = TextWise::API::Concept->new(API_TOKEN => $self->API_TOKEN);

	my $categories = $categorizer->process($self->TARGET_URL);
	my $concepts = $conceiver->process($self->TARGET_URL);

	my $ret = { categories => $categories, concepts => $concepts };
	# XXX mongo map reduce
	log_debug { "TODO: Map-reduce, return query results" };

	return $ret;
}

1;
