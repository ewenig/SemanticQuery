package TextWise::Data::URL;

use strict;
use warnings;
use Mouse;

# attributes
has 'API_TOKEN'  => (is => 'ro', isa => 'Str');
has 'TARGET_URL' => (is => 'ro', isa => 'Str');

use TextWise::API::Category;
use TextWise::API::Concept;

sub process {
	my $self = shift;
	my $categorizer = TextWise::API::Category->new(API_TOKEN => $self->API_TOKEN);
	my $conceiver = TextWise::API::Concept->new(API_TOKEN => $self->API_TOKEN);

	my $categories = $categorizer->process($self->TARGET_URL);
	my $concepts = $conceiver->process($self->TARGET_URL);

	my $ret = { categories => $categories, concepts => $concepts };
	# XXX insert into db

	return $ret;
}

1;
