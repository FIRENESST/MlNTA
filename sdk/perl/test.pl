#!/usr/bin/env perl
# perl -Ilib test.pl   (BEEP_GATEWAY env var)
use strict; use warnings;
use FindBin; use lib "$FindBin::Bin/lib";
use Minta::Beep;
my $fail = 0;
sub check { my ($c, $m) = @_; unless ($c) { $fail++; print "FAIL: $m\n" } }
my $c = Minta::Beep->new;
my $r = $c->call('POST', '/echo', { 'Content-Type' => 'text/plain', 'X-Test' => 'perl' }, 'hello from perl');
check($r->{status} == 200 && $r->{body} eq 'hello from perl' && $r->{headers}{'x-test'} eq 'perl', "echo $r->{status} $r->{body}");
$r = $c->get('/stream/3');
check($r->{body} eq "line 0\nline 1\nline 2\n", 'stream');
eval { $c->get('/err/537'); 1 } ? check(0, 'expected error') : check(ref $@ && $@->{code} == 537 && $@->{status} == 403, "err $@");
$r = $c->get('/status/404');
check($r->{status} == 404, 'status');
print $fail ? "Perl: $fail FAILURE(S)\n" : "Perl (gateway): all tests passed\n";
exit($fail ? 1 : 0);
