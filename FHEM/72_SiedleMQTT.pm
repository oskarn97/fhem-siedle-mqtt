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
    $hash->{AttrList} = "IODev qos retain cmnds " . $main::readingFnAttributes;
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
    	my $cmdList = "save ";
	    
        if(defined $main::attr{$name}{cmnds}) {
        	my @cmnds = split ' ', $main::attr{$name}{cmnds};
        	foreach(@cmnds) {
        		my @parts = split ':', $_;
        		$cmdList .= " ". $parts[0] . ":noArg";
        	}
        }

        return "Unknown argument " . $command . ", choose one of ". $cmdList;
    }

    if($command eq 'save') {
        my $value = formatCommand($values[1]) if(scalar @values == 2);
        $value = ReadingsVal($name, 'cmnd_value', undef) if(!defined $value);
        if(defined $value && !defined getCommand($hash, $value)) {
            my $cmnds = AttrVal($name, 'cmnds', '');
            $cmnds .= " " if(length $cmnds > 0);
            $cmnds .= $values[0] . ':' . $value;
            CommandAttr(undef, $name . ' cmnds ' . $cmnds);
            CommandSave(undef, undef);
            return undef;
        }
        return 'wrong syntax: set <name> save <command name>  [ <command> ]' if(scalar @values == 0);
    }

    my $exec = undef;

    my $retain = $hash->{".retain"}->{'*'};
    my $qos = $hash->{".qos"}->{'*'};
    my $value = join (" ", @values);
    my $values = @values;

    my @infos = getCommand($hash, $command);
    if(@infos && scalar @infos == 2) {
        $exec = formatCommand($infos[1]);
    }

    $exec = formatCommand($command) if(!defined $exec);

    if(defined $exec) {
        readingsBeginUpdate($hash);
        readingsBulkUpdate($hash, 'exec', defined $infos[0] ? $infos[0] : 'unknown');
        readingsBulkUpdate($hash, 'exec_value', $exec);
        readingsEndUpdate($hash, 1);
    	send_publish($hash->{IODev}, topic => 'siedle/cmnd/exec', message => $exec, qos => $qos, retain => $retain);
    }
}

sub getCommand($$) {
    my ($hash, $id) = @_;
    my $name = $hash->{NAME};
    if(defined $main::attr{$name}{cmnds}) {
        my @cmnds = split ' ', $main::attr{$name}{cmnds};
        foreach(@cmnds) {
            my @parts = split ':', $_;
            if($parts[0] eq $id || (scalar @parts == 2 && $parts[1] eq $id)) {
                return @parts;
            }
        }
    }

    return undef;
}

sub formatCommand($) {
    my ($cmnd) = @_;
    $cmnd = uc $cmnd;
    $cmnd =~ /(0X)?([0-9A-F]{8})/;
    return $2;
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
        if($path eq 'cmnd') {
            my @infos = getCommand($hash, $message);
            readingsBulkUpdate($hash, 'cmnd', defined $infos[0] ? $infos[0] : "unknown");
            readingsBulkUpdate($hash, 'cmnd_value', $message);
        } elsif($path eq 'state' && $message eq 'online') {
            main::InternalTimer(main::gettimeofday()+80, "SiedleMQTT::DEVICE::connectionTimeout", $hash, 1);
            $hash->{lastHeartbeat} = time();
            readingsBulkUpdate($hash, $path, $message) if(ReadingsVal($hash->{NAME}, 'state', '') ne 'online');
        } else {
            readingsBulkUpdate($hash, $path, $message);
        }
    	
        readingsEndUpdate($hash, 1);
    }
}

sub connectionTimeout {
    my ($hash) = @_;
    return if(time() - $hash->{lastHeartbeat} < 70);
    readingsSingleUpdate($hash, 'state', 'offline', 1);
}

1;