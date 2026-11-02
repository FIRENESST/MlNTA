package Minta::Beep;
# Minta::Beep — Perl client for BEEP backends via the beepgw HTTP gateway. Core modules only.
#
#   use Minta::Beep;
#   my $c = Minta::Beep->new(gateway => 'http://127.0.0.1:8080');
#   my $r = $c->call('POST', '/orders', { 'Content-Type' => 'application/json' }, '{"id":7}');
#   print $r->{status}, " ", $r->{body}, "\n";
#
# A BEEP ERR dies with a Minta::Beep::Error ({code, status, message}) unless no_die => 1.
use strict;
use warnings;
use HTTP::Tiny;

our $VERSION = '0.1.0';

sub new {
    my ($class, %o) = @_;
    my $self = bless {
        gateway  => ($o{gateway} || $ENV{BEEP_GATEWAY} || 'http://127.0.0.1:8080'),
        upstream => $o{upstream},
        no_die   => $o{no_die},
    }, $class;
    # HTTP::Tiny reads only lowercase no_proxy; honour NO_PROXY too and never proxy loopback.
    my @no_proxy = split /\s*,\s*/, ($ENV{no_proxy} // $ENV{NO_PROXY} // '');
    push @no_proxy, 'localhost', '127.0.0.1', '::1';
    $self->{http} = HTTP::Tiny->new(timeout => $o{timeout} || 60, keep_alive => 1, no_proxy => \@no_proxy);
    $self->{gateway} =~ s{/+$}{};
    return $self;
}

sub call {
    my ($self, $method, $target, $headers, $body, %o) = @_;
    my %h = %{ $headers || {} };
    my $up = $o{upstream} || $self->{upstream};
    $h{'X-Beep-Upstream'} = $up if $up;
    $h{'X-Beep-Timeout-Ms'} = $o{timeout_ms} if $o{timeout_ms};
    my $res = $self->{http}->request(uc $method, $self->{gateway} . $target,
        { headers => \%h, (defined $body ? (content => $body) : ()) });
    my $r = { status => $res->{status}, headers => $res->{headers}, body => $res->{content} };
    if ($res->{status} == 599) {
        die Minta::Beep::Error->new(code => 421, status => 599, message => $res->{content});
    }
    if (my $code = $res->{headers}{'x-beep-error-code'}) {
        my ($msg) = ($res->{content} // '') =~ /"message"\s*:\s*"((?:[^"\\]|\\.)*)"/;
        my $err = Minta::Beep::Error->new(code => $code, status => $res->{status}, message => $msg // $res->{content});
        die $err unless $self->{no_die} || $o{no_die};
        $r->{error} = $err;
    }
    return $r;
}

sub get  { my ($s, $t, %o) = @_; $s->call('GET', $t, $o{headers}, undef, %o) }
sub post { my ($s, $t, $b, %o) = @_; $s->call('POST', $t, $o{headers}, $b, %o) }

package Minta::Beep::Error;
use overload '""' => sub { "beep error $_[0]{code} (HTTP $_[0]{status}): $_[0]{message}" }, fallback => 1;
sub new { my ($c, %a) = @_; bless {%a}, $c }

1;
