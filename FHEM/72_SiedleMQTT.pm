##############################################
#
# fhem siedle mqtt gateway (see http://mqtt.org)
#
# written 2018 by Oskar Neumann
# thanks to Matthias Kleine
#
##############################################

use strict;
use warnings;

sub SiedleMQTT_Initialize($) {
    my $hash = shift @_;

    # Consumer
    $hash->{DefFn} = "SiedleMQTT::DEVICE::Define";
    $hash->{UndefFn} = "SiedleMQTT::DEVICE::Undefine";
    $hash->{SetFn} = "SiedleMQTT::DEVICE::Set";
    #$hash->{GetFn} = "SiedleMQTT::DEVICE::Get";
    $hash->{AttrFn} = "SiedleMQTT::DEVICE::Attr";
    $hash->{AttrList} = "IODev qos retain " . $main::readingFnAttributes;
    $hash->{OnMessageFn} = "SiedleMQTT::DEVICE::onmessage";

    main::LoadModule("MQTT");
    main::LoadModule("MQTT_DEVICE");
}

package SiedleMQTT::DEVICE;

use strict;
use warnings;
use POSIX;
use SetExtensions;
use GPUtils qw(:all);

use Net::MQTT::Constants;
use JSON;


BEGIN {
    MQTT->import(qw(:all));
    GP_Import(qw(
        CommandSave
        CommandDeleteReading
        CommandAttr
        readingsSingleUpdate
        readingsBulkUpdate
        readingsBeginUpdate
        readingsEndUpdate
        Log3
        fhem
        defs
        AttrVal
        ReadingsVal
    ))
};

sub Define() {
    my ($hash, $def) = @_;
    my @args = split("[ \t]+", $def);

    my ($name, $type) = @args;

    $hash->{TYPE} = 'MQTT_DEVICE';
    MQTT::Client_Define($hash, $def);
    $hash->{TYPE} = $type;

    if(defined($main::attr{$name}{IODev})) {
        SubscribeReadings($hash);
    }

    main::InternalTimer(main::gettimeofday()+80, "SiedleMQTT::DEVICE::connectionTimeout", $hash, 1) if($main::init_done);

    return undef;
};

sub Attr($$$$) {
    my ($command, $name, $attribute, $value) = @_;
    my $hash = $defs{$name};

    my $result = MQTT::DEVICE::Attr($command, $name, $attribute, $value);

    if ($attribute eq "IODev") {
        #SubscribeReadings($hash);
    }

    return $result;
}

sub SubscribeReadings {
    my ($hash) = @_;
    my ($mqos, $mretain, $mtopic, $mvalue, $mcmd) = MQTT::parsePublishCmdStr("siedle/#");
    client_subscribe_topic($hash, $mtopic, $mqos, $mretain);
}

sub Undefine($$) {
    my ($hash, $name) = @_;

    client_unsubscribe_topic($hash, 'siedle/#');
    RemoveInternalTimer($hash);

    return MQTT::Client_Undefine($hash);
}

sub Set($$$@) {
    my ($hash, $name, $command, @values) = @_;

    if ($command eq '?') {
    	my $cmdList = "open:noArg light:noArg ring ";
        return "Unknown argument " . $command . ", choose one of ". $cmdList;
    }

    if($command eq 'ring') {
        return 'wrong syntax: set <name> ring <phoneAddress>' if(scalar @values != 1);
        $command .= '_' . $values[0];
    }

    my $retain = $hash->{".retain"}->{'*'};
    my $qos = $hash->{".qos"}->{'*'};
    send_publish($hash->{IODev}, topic => 'siedle/exec', message => $command, qos => $qos, retain => $retain);
}

sub Get($$$@) {
    my ($hash, $name, $command, @values) = @_;

    #if ($command eq '?') {
    #    return "Unknown argument " . $command . ", choose one of " . join(" ", map { "$_$gets{$_}" } keys %gets) . " " . join(" ", map {$hash->{gets}->{$_} eq "" ? $_ : "$_:".$hash->{gets}->{$_}} sort keys %{$hash->{gets}});
    #}
}

sub onmessage($$$) {
    my ($hash, $topic, $message) = @_;

    Log3($hash->{NAME}, 5, "received message '" . $message . "' for topic: " . $topic);
    my @parts = split('/', $topic);
    my $path = $parts[-1];

    if(scalar @parts == 2) {
        readingsBeginUpdate($hash);
        if($path eq 'state' && $message eq 'online') {
            main::InternalTimer(main::gettimeofday()+80, "SiedleMQTT::DEVICE::connectionTimeout", $hash, 1);
            $hash->{lastHeartbeat} = time();
            readingsBulkUpdate($hash, $path, $message) if(ReadingsVal($hash->{NAME}, 'state', '') ne 'online');
        } elsif($path eq 'cmnd' || $path eq 'exec') {
            #do nothing, we use the datagram
        } else {
            readingsBulkUpdate($hash, $path, $message);
        }
    	
        readingsEndUpdate($hash, 1);
    }

    if(scalar @parts == 3) {
        readingsBeginUpdate($hash);
        if($path eq 'datagram') {
            if($parts[-2] eq 'result' || $parts[-2] eq 'cmnd') {
                my $key = $parts[-2] eq 'result' ? 'exec' : $parts[-2];
                Decode($hash, $message,  $key . "_");
                my $json = eval { JSON->new->utf8(0)->decode($message) };
                readingsBulkUpdate($hash, $key, $json->{spelling});
            } 
        }
        readingsEndUpdate($hash, 1);
    }
}

sub connectionTimeout {
    my ($hash) = @_;
    return if(time() - $hash->{lastHeartbeat} < 70);
    readingsSingleUpdate($hash, 'state', 'offline', 1);
}

sub Decode {
    my ($hash, $value, $prefix) = @_;
    my $h;

    eval {
        $h = JSON::decode_json($value);
        1;
    };

    if ($@) {
        Log3($hash->{NAME}, 2, "bad JSON: $value - $@");
        return undef;
    }

    Expand($hash, $h, $prefix);

    return undef;
}

sub Expand {
    my ($hash, $ref, $prefix, $suffix) = @_;

    $prefix = "" if (!$prefix);
    $suffix = "" if (!$suffix);
    $suffix = "-$suffix" if ($suffix);

    if (ref($ref) eq "ARRAY") {
        while (my ($key, $value) = each @{$ref}) {
            SiedleMQTT::DEVICE::Expand($hash, $value, $prefix . sprintf("%02i", $key + 1) . "-", "");
        }
    } elsif (ref($ref) eq "HASH") {
        while (my ($key, $value) = each %{$ref}) {
            if (ref($value) && !(ref($value) =~ m/Boolean/)) {
                SiedleMQTT::DEVICE::Expand($hash, $value, $prefix . $key . $suffix . "-", "");
            } else {
                # replace illegal characters in reading names
                (my $reading = $prefix . $key . $suffix) =~ s/[^A-Za-z\d_\.\-\/]/_/g;
                if(ref($value) =~ m/Boolean/) {
                    $value = $value ? "true" : "false";
                }

                readingsBulkUpdate($hash, lc($reading), $value);
            }
        }
    }
}

1;