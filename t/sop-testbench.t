#!/usr/bin/perl

use strict;
use warnings;
use Test::More tests => 100;
use FindBin qw($Bin);
use lib "$Bin/lib";
use Time::HiRes qw(gettimeofday tv_interval);
my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;
my $cmd;
my $val;
my $rst;
use MemcachedTest;

my $total_elem = 10000;
my $nelem = 5000;
my $num_tests = 100;
sub sop_insert {
    my ($key, $from, $to, $create) = @_;
    my $index;
    my $vleng;

    my $tmpcmd;
    my $tmprst;
    # $tmpcmd = "config max_set_size 90000";
    # $tmprst = "END";``
    # mem_cmd_is($sock, $tmpcmd, "", $tmprst);
    # diag("config done\n");

    for ($index = $from; $index <= $to; $index++) {
        my $val = "datum$index";
        $vleng = length($val);
        my $cmd;
        my $rst;
        if ($index == $from) {
            $cmd = "sop insert $key $vleng $create";
            $rst = "CREATED_STORED";
        } else {
            $cmd = "sop insert $key $vleng";
            $rst = "STORED";
        }
        mem_cmd_is_quiet($sock, $cmd, $val, $rst);
        # diag($index);
    }
    # diag("insert done\n");
    for (my $i = 1; $i <= $total_elem; $i++){
        my $line = scalar <$sock>;
    }
}
sub sop_get_is_quiet {
    my ($sock_opts, $args, $flags, $ecount, $values, $msg) = @_;
    my $opts = ref $sock_opts eq "HASH" ? $sock_opts : {};
    my $sock = ref $sock_opts eq "HASH" ? $opts->{sock} : $sock_opts;

    print $sock "sop get $args\r\n";

    my $ok_header = "VALUE $flags $ecount\r\n";
    my $server_count = $1 || 0;
    my $got_header = "";
    $got_header = scalar <$sock>;
    # diag("got header: $got_header");

    my $actual_element_count = 0;
    my $line = scalar <$sock>;
    # diag("$actual_element_count:  $line");
    while (defined $line and $line !~ /^END/ and $line !~ /^TRIMMED/ and $line !~ /^DELETED/ and $line !~ /^DELETED_DROPPED/) {
        $actual_element_count++;
        $line = scalar <$sock>;
        # diag("$actual_element_count:  $line");
    }
    Test::More::is($got_header, $ok_header);
}

sub mem_cmd_is_quiet {
    my ($sock_opts, $cmd, $val, $rst, $msg) = @_;
    my @response_list;
    my @response_error = ("ATTR_ERROR", "CLIENT_ERROR", "ERROR", "PREFIX_ERROR", "SERVER_ERROR");

    my @prdct_response = split('\n', $rst);
    my @cmd_pipeline = split('\r\n', $cmd);
    my $rst_type = 0;
    my $count;

    my $opts = ref $sock_opts eq "HASH" ? $sock_opts : {};
    my $sock = ref $sock_opts eq "HASH" ? $opts->{sock} : $sock_opts;

    # send command
    if ("$val" eq "") {
        print $sock "$cmd\r\n";
    } else {
        print $sock "$cmd\r\n$val\r\n";
    }
}

sub sop_get {
    my ($args, $flags, $ecount, $from, $to) = @_;
    my $index;
    my @res_data = ();
    for ($index = $from; $index <= $to; $index++) {
        push(@res_data, "datum$index");
    }
    my $data_list = join(",", @res_data);
    sop_get_is_quiet($sock, $args, $flags, $ecount, $data_list);
}

my $flags  = 13;
my $start_time = [gettimeofday];

sop_insert("skey", 0, $total_elem-1, "create $flags 0 -1");
diag("Inserted $total_elem number of elements.");

my $elapsed = tv_interval($start_time);
# open(my $fh, '>', 'report.txt');
my $sum_time = 0;
for my $i (1 .. $num_tests) {
    $start_time = [gettimeofday];
    sop_get("skey $nelem", $flags, $nelem, 0, $nelem - 1);
    $elapsed = tv_interval($start_time);
    $elapsed = sprintf("%.7f", $elapsed);
    diag("Trial $i: SOP GET of $nelem elements took $elapsed seconds.\n");
    $sum_time += $elapsed;
}

my $avg_time = $sum_time / $num_tests;
my $decimal_time = sprintf("%.7f", $avg_time);
diag("Average SOP GET of $nelem elements over $num_tests trials: $decimal_time seconds.\n");
# close $fh;

release_memcached($engine, $server);
