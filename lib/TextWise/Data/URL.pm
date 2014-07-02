package TextWise::Data::URL;

use strict;
use warnings;
use Carp;
use Mouse;

# attributes
has 'API_TOKEN'  => (is => 'ro', isa => 'Str');
has 'TARGET_URL' => (is => 'ro', isa => 'Str', required => 0);

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
	# XXX insert into db

	return $ret;
}

1;
