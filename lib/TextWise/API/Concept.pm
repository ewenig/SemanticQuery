package TextWise::API::Concept;

use 5.010;
use Mouse;

# attributes
has 'API_TOKEN' => (is => 'ro', isa => 'Str');
has 'API_OPTS'  => (is => 'rw', isa => 'HashRef', required => 0);

use Carp;
use URL::Encode qw(url_encode);
use WWW::Curl::Easy;
use TextWise;
use JSON;
use Data::Dumper qw(Dumper);

sub process {
	my $self = shift;
	my $TARGET_URL = shift or croak(__PACKAGE__ . " requires a URL argument");
	my $TW_API_BASE = "http://api.semantichacker.com/" . $self->API_TOKEN . "/concept?";
	my $curl = WWW::Curl::Easy->new;

	#options construction
	my $opts;
	if (defined($self->API_OPTS)) { $opts = $self->API_OPTS; }
	else { # sane defaults
		$opts = {};
	}
	$opts->{'uri'} = $TARGET_URL;
	$opts->{'format'} = 'json'; # non-negotiable

	#url construction
	my $url = $TW_API_BASE . _makeopts($opts);

	$curl->setopt(CURLOPT_URL, $url);

	# var to hold the result
	my $resp;
	$curl->setopt(CURLOPT_WRITEDATA,\$resp);

	# perform request
	$curl->perform;

	# error checking
	my $respobj = decode_json($resp);
	if ($respobj->{'message'} && $respobj->{'message'}->{'messageCode'} == 102) {
		croak("API call failed: " . $respobj->{'message'}->{'messageText'});
	}

	my $concepts = $respobj->{'conceptExtractor'}->{'conceptExtractorResponse'}->{'concepts'};
	return $concepts;
}

## helper functions ##
# take in a hashref, output URI-friendly HTTP GET string
sub _makeopts {
	my $opts = shift;
	my $get_str = "";

	for (keys %$opts) {
		$get_str .= sprintf("%s=%s&",$_,url_encode($opts->{$_}));
	}

	chop $get_str; #remove trailing &
	return $get_str;
}

=head1 NAME

TextWise::API::Concept - Perl interface for the TextWise (http://www.textwise.com/) Concept API

=head1 VERSION

Version 0.01

=cut

our $VERSION = '0.01';


=head1 SYNOPSIS

TextWise::API::Concept provides native access to the TextWise Concept API.

It is invoked thusly:

    use TextWise::API::Concept;;

    my $con = TextWise::API::Concept->new(
        API_TOKEN = 'tokenstring',  # required
	);

    $resp = $con->process('http://www.example.com'); # response in Array[HashRef] format

=head1 AUTHOR

Eli Wenig, C<< <eli at csh.rit.edu> >>



=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc TextWise::API::Concept


You can also look for information at:

=over 4

=item * RT: CPAN's request tracker (report bugs here)

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=TextWise-API-Concept>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/TextWise-API-Concept>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/TextWise-API-Concept>

=item * Search CPAN

L<http://search.cpan.org/dist/TextWise-API-Concept/>

=back


=head1 ACKNOWLEDGEMENTS


=head1 LICENSE AND COPYRIGHT

Copyright 2014 Eli Wenig.

This program is free software; you can redistribute it and/or modify it
under the terms of the the Artistic License (2.0). You may obtain a
copy of the full license at:

L<http://www.perlfoundation.org/artistic_license_2_0>

Any use, modification, and distribution of the Standard or Modified
Versions is governed by this Artistic License. By using, modifying or
distributing the Package, you accept this license. Do not use, modify,
or distribute the Package, if you do not accept this license.

If your Modified Version has been derived from a Modified Version made
by someone other than you, you are nevertheless required to ensure that
your Modified Version complies with the requirements of this license.

This license does not grant you the right to use any trademark, service
mark, tradename, or logo of the Copyright Holder.

This license includes the non-exclusive, worldwide, free-of-charge
patent license to make, have made, use, offer to sell, sell, import and
otherwise transfer the Package with respect to any patent claims
licensable by the Copyright Holder that are necessarily infringed by the
Package. If you institute patent litigation (including a cross-claim or
counterclaim) against any party alleging that the Package constitutes
direct or contributory patent infringement, then this Artistic License
to you shall terminate on the date that such litigation is filed.

Disclaimer of Warranty: THE PACKAGE IS PROVIDED BY THE COPYRIGHT HOLDER
AND CONTRIBUTORS "AS IS' AND WITHOUT ANY EXPRESS OR IMPLIED WARRANTIES.
THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
PURPOSE, OR NON-INFRINGEMENT ARE DISCLAIMED TO THE EXTENT PERMITTED BY
YOUR LOCAL LAW. UNLESS REQUIRED BY LAW, NO COPYRIGHT HOLDER OR
CONTRIBUTOR WILL BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, OR
CONSEQUENTIAL DAMAGES ARISING IN ANY WAY OUT OF THE USE OF THE PACKAGE,
EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


=cut

1; # End of TextWise::API::Concept

