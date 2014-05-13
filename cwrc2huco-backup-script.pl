#!/usr/bin/perl
$| = 1; # turn off output buffering

# TODO: clean up recursive code (ie the big file-by-file code should not just sit inside the big bag if

use strict;
use warnings;

use File::Find; # to find files (duh): http://perldoc.perl.org/File/Find.html
use Digest::MD5; # to calculate md5s : http://perldoc.perl.org/Digest/MD5.html

use LWP::Simple; # to read the response back from the server?

use List::Util qw(first); # for the find index in the server state function

use POSIX qw(strftime); # http://perldoc.perl.org/functions/localtime.html

use HTTP::Date; # for parsing the date/time when seeing how old the status is

# states
# 1) never heard of it (or Weiwei has uploaded it). I think the first one should be location-CWRC
# 2) md5 confirmed
# 3) conversions started
# 4) conversion finished
# 5) upload

# V.10: destination changed to Huco and added metadata_hostname, put it in getURL
my $destination_path = "/var/sites/emic-cms/upload/";
my $destination_user = "mpb";
# V.10: removed the http://
my $destination_hostname = "arrl-web001.artsrn.ualberta.ca";

my $metadata_hostname = "142.244.99.9";


#my $destination_hostname = "http://www.artsrn.ualberta.ca/emic-cms/";
#my $destination_path = "/home/Projects/emic/cwrc-uploads/";
#my $destination_user = "emic";
#my $destination_hostname = "huco.artsrn.ualberta.ca";
#my $destination_hostname = "142.244.99.9";

#my $source_path = "/data/home/emic/test-delme/"; for testing
#my $source_path = "/data/home/emic/bags/";
my $source_path = "/data/home/emic/delme-2013-12-07/";

my $debug = "true"; # if true, ensures that no changes are made to the server, just status messages printed
my $debug_file_state = "CWRC-createpng-end"; # set file state for the purpose of debugging
#my $debug_bag_state = "UAL-run-end"; # set file state for the purpose of debugging 
my $debug_bag_state = "CWRC-verifybag-end";
my $debug_bag_state_date = " 2013-10-27 14:34:10";

my $logging = "true";
my $current_log_file;

# V.11: added a fail state for in-bag, file-level transactions. This way, if a file fails, the bag continues, but we know that one file has failed, so the bag CANNOT move to the next bag state. At the mo, a file can fail, but the bag can be incorrectly set to complete. This will be updated on file-level errors and it will be checked before bag state is changed to CWRC-pngbag-end
my $file_error_in_bag;

#my @stored_bag_status;

my @file_states = ( "false",
					"CWRC-file-process-start",  
                	"CWRC-file-createpng-start", 
                	"CWRC-file-createpng-end", 
                	"CWRC-file-upload-start", 
                	"CWRC-file-upload-end",
                	"CWRC-file-process-end"
                );
                
my @bag_states = (	"CWRC-verifybag-false",
					"UAL-run-end",
					"CWRC-run-start",
					"CWRC-unsplitbag-start",
					"CWRC-unsplitbag-end",
					"CWRC-untarbag-start",
					"CWRC-untarbag-end",
					"CWRC-verifybag-start",
					"CWRC-verifybag-end",
					"CWRC-pngbag-start",
					"CWRC-pngbag-end",
					"CWRC-run-end",
					
					"UAL-run-end",
					"UAL-boxnameadd-start",
					"UAL-boxnameadd-end",
					"UAL-jhove-start",
					"UAL-jhove-end",
					"UAL-jhoveparse-start",
					"UAL-jhoveparse-end",
					"UAL-badtiff",
					"UAL-createbag-start",
					"UAL-createbag-end",
					"UAL-verifybag-start",
					"UAL-verifybag-end",
					"UAL-uploadbag-start",
					"UAL-uploadbag-end",
				);

# Check for new bags
find (\&bags_to_action, $source_path); # grab all bags in run
    sub bags_to_action {

if ($_ =~ ".DS_Store" || $_ =~ /xml$/ || $_ =~ /txt$/ || $_ =~ /png$/ || $_ =~ /log$/ || $_ =~ /gz/) { # ignore ds_store, txt,  xml, png files. Ignore the bag files (that contain gz) also
	return 0;
}
    	
my $bag_filename = $_.""; #for adding a 1 or something to the filename so we can test more with fewer files
my $bag_path = $File::Find::dir;
my $bag_id;

# *** Local state and server state are separate to reduce the number of server hits. we only really need to check the server status once. otherwise, we can keep track internally
#my $local_file_state;
my $local_bag_state;
my $local_bag_state_corrected = "false";

my $server_file_state;
my $server_bag_state;

# is it a bag?
#if ($bag_filename =~ /[0-9]{2}\-[0-9]*\-[0-9]*\-[0-9]*$/) { # 2 digits-NUM-NUM-NUM<END
if ($bag_filename =~ /[0-9]{2}\-[0-9]*\-[0-9]*\-[0-9]*.$/) { # 2 digits-NUM-NUM-NUM .. had to remove the "end" because of one folder with a stupid A behind it. so anychar, then end
	
	# set up the log
	#$current_log_file = strftime "%Y-%m-%d--%H-%M-%S", localtime;
	#$current_log_file = $bag_filename."/".$current_log_file;
	# Changed for version 8, all bags (or folders) have their own logs: bag_id.txt
	$current_log_file = $bag_filename."/log"; # 
	# Since all runs are in the same bag, we need a run indicator
	log_message ("[bag run start] [".$bag_filename."]\n\n\n\n");
	
	$server_bag_state = getStatus($bag_filename); #getStatus with one arg runs bag-status
	# next line from: http://stackoverflow.com/questions/1915746/in-perl-how-can-i-find-the-index-of-a-given-value-in-an-array	
	$local_bag_state = first { $bag_states[$_] eq $server_bag_state } 0..$#bag_states;
	if (!defined($local_bag_state)) { # if the returned state is a legit state, move on, if not quit
		log_message ("\n\n***STATE-ERROR: [".$bag_filename."] Undefined server file state (".$server_bag_state.")***\n");
		exit (1);
	}
	debug_message("[bag state report][".$bag_filename."] Local file state (from the server)".$local_bag_state);	
	$bag_id = $bag_filename; #it's a bag, so we should call it a bag_id
	#@stored_bag_status = bag_status_backup($bag_id); # since we're starting a bag backup, store the status
	#debug_message("test storing status for bag (".$bag_id."):\n".join("\n",@stored_bag_status)."\n\n");
}


#ignore files for the moment, what's next for a bag...?
if (defined ($local_bag_state) && $local_bag_state eq "1") { # it's  bag, and the last state was UAL-run-end
	#set to CWRC-run-start
    $local_bag_state++; # should be from 1 to 2
    debug_message("[bag state report][".$bag_filename."] Local state, inside the CWRC-run-start if: ".$local_bag_state."\n");
	if (setStatus("", $bag_id, $bag_states[$local_bag_state]) eq "false") { # dual function: sets state, checks for errors
		log_message ("[bag state change][".$bag_filename."] state was successfully changed to ".$bag_states[$local_bag_state]."\n");
	}
	else {
		# in this situation, we have caught a collision (ie the state was changed without us knowing it)
		log_message ("\n\n***STATE-ERROR: [".$bag_filename."] error caught while trying to change state to".$bag_states[$local_bag_state]."***\n");
		# skip this bag
		next;
	}

}

if (defined ($local_bag_state) && ($local_bag_state eq "3" || $local_bag_state eq "5" || $local_bag_state eq "7" || $local_bag_state eq "9")) {
	# if we are out here and you have one of these states it means you were interrupted
	# so, we subtract one from the state, to get it to re-run the failed state
	
	$local_bag_state--;
	log_message ("[bag state change][".$bag_filename."] Bag state error detected (interrupted process detected) state was successfully changed (back) to ".$bag_states[$local_bag_state]."\n");
	$local_bag_state_corrected = "true";
	
}

if (defined ($local_bag_state) && $local_bag_state eq "2") { # CWRC-run-start..means we want to unsplit!  "[bag state change]".
	#set to CWRC-unsplitbag-start
    $local_bag_state++;
    debug_message("[bag state report][".$bag_filename."] Local state, inside the CWRC-unsplitbag if: ".$local_bag_state."\n");
	if ($local_bag_state_corrected eq "true" || setStatus("", $bag_id, $bag_states[$local_bag_state]) eq "false") { # see NOTE #1:
		log_message ("[bag state change][".$bag_filename."] state was successfully changed to ".$bag_states[$local_bag_state]."\n");
		
		my $unsplit_command = "cat ".$bag_path."/".$bag_id."/"."split_bag-".$bag_id."_*.tar.gza* > ".$bag_path."/".$bag_id."/"."bag-".$bag_id.".tar.gz";
		debug_message("[command][".$bag_filename."] In CWRC-unsplitbag: Unsplit command: ".$unsplit_command."\n");
		my $unsplit_result = `$unsplit_command  2>&1`;
		debug_message("[command output][".$bag_filename."] In CWRC-unsplitbag: the unsplit command output: ".$unsplit_result."\n");
		# http://stackoverflow.com/questions/777543/how-can-i-read-the-error-output-of-external-commands-in-perl
	    my $errorReturn = "FAIL" if $?;
	    if (defined($errorReturn)) {
	    	setStatus("", $bag_id, $bag_states[0]); # set state to FAIL!
	    	log_message ("\n\n***COMMAND-ERROR: [".$bag_filename."] error caught while trying to unsplit this bag. Status set to 'fail' and skip this bag. Error code returned by the system: ".$errorReturn."***\n\n");
	    	next;
	    }
		
		#set to CWRC-unsplitbag-end
    	$local_bag_state++;
		setStatus("", $bag_id, $bag_states[$local_bag_state]);
		log_message ("[bag state change][".$bag_filename."] state was successfully changed to ".$bag_states[$local_bag_state]."\n");
	}
	else {
		# in this situation, we have caught a collision (ie the state was changed without us knowing it)
		log_message ("\n\n***STATE-ERROR: [".$bag_filename."] error caught while trying to change state to".$bag_states[$local_bag_state]."***\n\n");
		# skip this bag
		next;
	}
}

if (defined ($local_bag_state) && $local_bag_state eq "4") { # CWRC-unsplitbag-end..means we want to untar!
	#set to CWRC-untarbag-start
    $local_bag_state++;
    debug_message("[bag state report][".$bag_filename."] Local state, inside the CWRC-untarbag if: ".$local_bag_state."\n");
	if ($local_bag_state_corrected eq "true" || setStatus("", $bag_id, $bag_states[$local_bag_state]) eq "false") { # see NOTE #1:
		log_message ("[bag state change][".$bag_filename."] state was successfully changed to ".$bag_states[$local_bag_state]."\n");
		
		my $untar_command = "tar -C ".$bag_path."/".$bag_id."/ -xvf ".$bag_path."/".$bag_id."/"."bag-".$bag_id.".tar.gz";
		debug_message("[command][".$bag_filename."] In CWRC-untarbag: Untar command: ".$untar_command."\n");
		my $untar_result = `$untar_command  2>&1`;
		debug_message("[command output][".$bag_filename."] In CWRC-untarbag: the untar command output: ".$untar_result."\n");
		
		# http://stackoverflow.com/questions/777543/how-can-i-read-the-error-output-of-external-commands-in-perl
	    my $errorReturn = "FAIL" if $?;
	    if (defined($errorReturn)) {
	    	setStatus("", $bag_id, $bag_states[0]); # set state to FAIL!
	    	log_message ("\n\n***COMMAND-ERROR: [".$bag_filename."] error caught while trying to untar this bag. Status set to 'fail' and skip this bag. Error code returned by the system: ".$errorReturn."***\n\n");
	    	next;
	    }
		
		#set to CWRC-untarbag-end
    	$local_bag_state++;
		setStatus("", $bag_id, $bag_states[$local_bag_state]);
		log_message ("[bag state change][".$bag_filename."] state was successfully changed to ".$bag_states[$local_bag_state]."\n");
	}
	else {
		# in this situation, we have caught a collision (ie the state was changed without us knowing it)
		log_message ("\n\n***STATE-ERROR: [".$bag_filename."] error caught while trying to change state to".$bag_states[$local_bag_state]."***\n\n");
		# skip this bag
		next;
	}
}

if (defined ($local_bag_state) && $local_bag_state eq "6") { # CWRC-untar-end..means we want to verify!
	#set to CWRC-verifybag-start
    $local_bag_state++;
    debug_message("[bag state report][".$bag_filename."] Local state, inside the CWRC-verifybag if: ".$local_bag_state."\n");
	if ($local_bag_state_corrected eq "true" || setStatus("", $bag_id, $bag_states[$local_bag_state]) eq "false") { # see NOTE #1:
		log_message ("[bag state change][".$bag_filename."] state was successfully changed to ".$bag_states[$local_bag_state]."\n");
		
		my $verify_command = "/usr/bin/bagit.py --validate ".$bag_path."/".$bag_id."/";
		debug_message("[command][".$bag_filename."] In CWRC-verifybag: Verify command: ".$verify_command."\n");
		my $verify_result = `$verify_command  2>&1`;
		debug_message("[command output][".$bag_filename."] In CWRC-verifybag: the verify command output: ".$verify_result."\n");
		
		# http://stackoverflow.com/questions/777543/how-can-i-read-the-error-output-of-external-commands-in-perl
	    my $errorReturn = "FAIL" if $?;
	    if (defined($errorReturn)) {
	    	setStatus("", $bag_id, $bag_states[0]); # set state to FAIL!
	    	log_message ("\n\n***COMMAND-ERROR: [".$bag_filename."] error caught while trying to verify this bag. Status set to 'fail' and skip this bag. \nOutput: ".$verify_result."\nError code returned by the system: ".$errorReturn."***\n\n");
	    	next;
	    }
		
		#set to CWRC-verifybag-end
    	$local_bag_state++;
		setStatus("", $bag_id, $bag_states[$local_bag_state]);
		log_message ("[bag state change][".$bag_filename."] state was successfully changed to ".$bag_states[$local_bag_state]."\n");
	}
	else {
		# in this situation, we have caught a collision (ie the state was changed without us knowing it)
		log_message ("\n\n***STATE-ERROR: [".$bag_filename."] error caught while trying to change state to".$bag_states[$local_bag_state]."***\n\n");
		# skip this bag
		next;
	}
}

if (defined ($local_bag_state) && $local_bag_state eq "8") { # CWRC-verifybag-end..means we want to png!
	#set to CWRC-pngbag-start
    $local_bag_state++;
    debug_message("[bag state report][".$bag_filename."] Local state, inside the CWRC-pngbag if: ".$local_bag_state."\n");
	if ($local_bag_state_corrected eq "true" || setStatus("", $bag_id, $bag_states[$local_bag_state]) eq "false") { # see NOTE #1:
		log_message ("[bag state change][".$bag_filename."] state was successfully changed to ".$bag_states[$local_bag_state]."\n");
		
		
		$file_error_in_bag = "false"; # V.11: this is a global variable that we set to default false. If no errors occur, it should stay false.
		# grab all the files
		find (\&png_that_bag, $bag_path."/".$bag_id); # grab all files in this bag

		
		#set to CWRC-pngbag-end
		if ($file_error_in_bag eq "false") { # V.11: Added this to prevent the bag state from continuing if files failed their processes. So, if there are no errors (file_error_in_bag == "false") bag state is set to CWRC-pngbag-end
			$local_bag_state++;
			setStatus("", $bag_id, $bag_states[$local_bag_state]);
			log_message ("[bag state change][".$bag_filename."] state was successfully changed to ".$bag_states[$local_bag_state]."\n");
		}
		else { # V.11: If (file_error_in_bag is not false, hopefully true), do not change the state, skip this bag and try aain next time
			log_message ("[bag state note][".$bag_filename."] has not been completed and was skipped because one or more of the file processes failed. State remains as ".$bag_states[$local_bag_state]." \n");
			# skip this bag
			next;
		}
	}
	else {
		# in this situation, we have caught a collision (ie the state was changed without us knowing it)
		log_message ("\n\n***STATE-ERROR: [".$bag_filename."] error caught while trying to change state to".$bag_states[$local_bag_state]."***\n\n");
		# skip this bag
		next;
	}
}

if (defined ($local_bag_state) && $local_bag_state eq "10") { # CWRC-pngbag-end..means we are done!
	
	# set to CWRC-run-end
	$local_bag_state++;
	setStatus("", $bag_id, $bag_states[$local_bag_state]);
	log_message ("[bag state change][".$bag_filename."] state was successfully changed to ".$bag_states[$local_bag_state]."\n\n\n");
	
	if ($debug eq "false") {
		# delete the unsplit and the split bags (there is currently no state change for this)
		my $delete_command = "rm -f ".$bag_path."/".$bag_id."/*.tar.g*";
		debug_message("[command][".$bag_filename."] In CWRC-run-end: Delete Tars command: ".$delete_command."\n");
		my $delete_result = `$delete_command  2>&1`;
		debug_message("[command output][".$bag_filename."] In CWRC-run-end: the Delete Tars command output: ".$delete_result."\n");
		
		# http://stackoverflow.com/questions/777543/how-can-i-read-the-error-output-of-external-commands-in-perl
		my $errorReturn = "FAIL" if $?;
		if (defined($errorReturn)) {
			log_message ("\n\n***COMMAND-ERROR: [".$bag_filename."] error caught while trying to delete the tars of this bag. Bag is still set to finished. \nOutput: ".$delete_result."\nError code returned by the system: ".$errorReturn."***\n\n");
			next;
		}
	}
}

if (defined ($local_bag_state) && $local_bag_state > "11") { # means have a valid "last" status, but it's not the right one!

	# ideally, if UAL is unfinished for more than 24 hrs, that means an error, and we can set the bag to fail so UAL will run it again.

	# make a quick check to see how long ago the last status was
	#http://www.linuxquestions.org/questions/programming-9/how-to-subtract-two-date-in-perl-600366/
	
	my $status_time = getStatusDate ($bag_id); #probably should get the bag-based vars...MATT
	my $current_time = time();
	
	my $time_lapsed_in_seconds  = $current_time - str2time($status_time);
	
	if ($time_lapsed_in_seconds > 86400) { # 24*60*60 or the number of seconds in a day
		# if the status is some non-CWRC status AND we've been waiting more than a day, it's probably a UAL error, so set it to "we want the bag" aka verifybag-false
		# also, because we just checking for 24 hrs we should avoid the problems of daylight savings time and locale time/date bugs. If we hit the wrong date/time, we'll get a negative number, so we'll just skip this and the state will be unchanged until we try again
	
		# set to CWRC-verifybag-false
		setStatus("", $bag_id, $bag_states[0]); # set state to FAIL!
		log_message ("\n\n***STATE-ERROR: [".$bag_filename."] this bag has an existing status but it is not one CWRC can deal with. Status set to 'fail'. Skip this bag.***\n\n");
		    	next;
	}
	else { # it's within 24 hours, so it's still in process
		log_message ("\n\n[".$bag_filename."] this bag has an existing status, it is not a CWRC one, but it's WITHIN 24 hours, so we're leaving it.\n\n");
		    	next;
	}
}

} # end bags_to_action

sub getStatus { # 0 is the filename, 1 is the path, or 0 is bag_id
	# always sleep a second before hitting the server:
	sleep (1);
	if (@_ == 2) { # if there are two arguments
		my $getUrl = "http://".$metadata_hostname."/~hquamen/watson/metadata/index.php?".
						"filename=".$_[0]."&".
						"path=".$_[1]."&".
						"action=get";
		my $status;
		if ($debug eq "false") {
			# http://stackoverflow.com/questions/11138438/open-remote-file-via-http	
			$status = get $getUrl;
			die "Invalid data in getUrl ($_[0], $_[1])" unless defined $status;
		}
		else {
			debug_message("[sub message: getStatus]"."getStatus (".$_[0].", ".$_[1]."):\nURL: ".$getUrl."\nGot the status, for debug mode, all states are returned (".$debug_file_state.")"."\n"); 
			$status = $debug_file_state;
		}
		#print "\n\ngetStatus (".$_[0].", ".$_[1]."):\nURL: ".$getUrl."\nStatus: ".$status."\n";
		if ($status eq "false") { return $status ; }
		else {
			my @temp = split (/ /, $status);
			return $temp[0];
		}
	} # end if @=2
	if (@_ == 1) { # if there is one argument (bag check)
		my $getUrl = "http://".$metadata_hostname."/~hquamen/watson/metadata/index.php?".
						"filename=&".
						"path=".$_[0]."&".
						"action=bag-get-status";
		my $status;
		if ($debug eq "false") {
			# http://stackoverflow.com/questions/11138438/open-remote-file-via-http	
			$status = get $getUrl;
			die "Invalid data in getUrl ($_[0])" unless defined $status;
		}
		else {
			debug_message("[sub message: getStatus]"."getStatus (".$_[0]."):\nURL: ".$getUrl."\nGot the status, for debug mode, all states are returned (".$debug_bag_state.")"."\n"); 
			$status = $debug_bag_state;
		}
		#print "\n\ngetStatus (".$_[0]."):\nURL: ".$getUrl."\nStatus: ".$status."\n";
		if ($status eq "false") { return $status ; }
		else {
			my @temp = split (/ /, $status);
			return $temp[0];
		}
	} # end if @=2
}

sub getStatusDate { # get the date/time from the last server status

	# always sleep a second before hitting the server:
	sleep (1);
	my $getUrl = "http://".$metadata_hostname."/~hquamen/watson/metadata/index.php?".
				"filename=&".
				"path=".$_[0]."&".
				"action=bag-get-status";
	my $status;
	if ($debug eq "false") {
		# http://stackoverflow.com/questions/11138438/open-remote-file-via-http	
		$status = get $getUrl;
		die "Invalid data in getUrl ($_[0])" unless defined $status;
	}
	else {
		debug_message("[sub message: getStatus]"."getStatus (".$_[0]."):\nURL: ".$getUrl."\nGot the status, for debug mode, all states are returned (".$debug_bag_state.")"."\n"); 
		$status = $debug_bag_state.$debug_bag_state_date; # append a time for date testing
	}
	#print "\n\ngetStatus (".$_[0]."):\nURL: ".$getUrl."\nStatus: ".$status."\n";
	if ($status eq "false") { return $status ; }
	else {
		my @temp = split (/ /, $status);
		return $temp[1]." ".$temp[2];
	}
	
}
sub setStatus { # 0 is the filename, 1 is the path, 2 is the new status)
	# always sleep a second before hitting the server:
	sleep (1);
	
	my $settingUrl = "http://".$metadata_hostname."/~hquamen/watson/metadata/index.php?".
					"filename=".$_[0]."&".
					"path=".$_[1]."&".
					"action=".$_[2];
	my $status;
	if ($debug eq "false") {
		# http://stackoverflow.com/questions/11138438/open-remote-file-via-http	
		$status = get $settingUrl;
		die "Invalid data in setStatus ($_[0], $_[1], $_[2])" unless defined $status;
	}
	else {
		debug_message("[sub message: setStatus]"."setStatus (".$_[0].", ".$_[1].", ".$_[2]."):\nURL: ".$settingUrl."\nSet the status to ".$_[2].", will return FALSE because this should be a NEW status for it"."\n"); 
		$status = "false";
	}
	#print "\n\nsetStatus (".$_[0].", ".$_[1].", ".$_[2]."):\nURL: ".$settingUrl."\nResult from server: ".$status."\n";
	return $status;

}

sub calculate_file_md5 {
	my $file = shift; # get me the first argument which should be a file
	open(FILE, $file) or die "Can't open '$file': $!";
	binmode(FILE); # file handle is in binary mode
	return Digest::MD5->new->addfile(*FILE)->hexdigest;
}

sub debug_message {
	if ($debug eq "true") {
		print "\n\nDEBUG: ".$_[0]." :DEBUG\n\n";	
	}
	log_message ($_[0]."\n");
}

sub bag_status_backup {
	# get me the first argument which should be a bag_id
	my $bag_id_to_backup = shift;
	
	# get me all the statuses for this bag (no filename for bags, bag_id, action to get all status)
	my $backup_status = setStatus ("", $bag_id_to_backup, "all");	
	
	my @backup_status_array;
	my $index = 0;
	#my @temp_array = split (/ |\n/, $backup_status); # split that array by whitespace
	foreach my $line (split (/ |\n/, $backup_status)) { # split on a "space" or a "newline"
		debug_message("bag_status_backup (".$bag_id_to_backup."): ".$line."\n");
		# every third item is a status (status[space]date[space]time[newline], so just add those to the status array
		if ($index % 3 == 0) { push (@backup_status_array, $line); } 
		$index++;
	}
	return @backup_status_array;
}

sub bag_status_restore {
	# this should probably have some checks (like catching what setStatus returns...
	# ALSO THIS IS UNTESTED because it doesn't fix timestamps yet	
	my $bag_id_to_restore = $_[0];
	my @status_to_restore = $_[1];
	
	if (@status_to_restore && @status_to_restore != "") {
		# clear current status
		setStatus("", $bag_id_to_restore, "clear"); # no filename for bags
		
		foreach my $status (@status_to_restore) {
			sleep (1);
			setStatus("", $bag_id_to_restore, $status);
		}
	}
	
}

sub log_message {
	# interesting discussion that I didn't end up using:
	# http://stackoverflow.com/questions/3822787/how-can-i-redirect-stdout-and-stderr-to-a-log-file-in-perl
	if ($logging) {
		use POSIX qw(strftime);
		my $log_timestamp = strftime "%Y-%m-%d %H:%M:%S", localtime;
		#open my $log_file_handle, '>>', $source_path.$start_time_stamp.".txt";
		#open my $log_file_handle, '>>', $source_path.$current_log_file.".txt"; #changed to get better errors
		open(my $log_file_handle, '>>', $source_path.$current_log_file.".txt") or die "Can't write to file (".$source_path.$current_log_file.".txt".") [$!]\n";
			print $log_file_handle ("[".$log_timestamp."] ".$_[0]);
		close $log_file_handle;
	}
	else {
		print $_;	
	}
}

sub png_that_bag {

	if ($_ =~ ".DS_Store" || $_ =~ /xml$/ || $_ =~ /txt$/ || $_ =~ /png$/ || $_ =~ /log$/ || $_ =~ /gz/ || $_ =~ /boxnameadded/) { 
		# ignore ds_store, txt,  xml, png, gz, boxnameadded files
		return 0;
	}
	if (-d $_) { # ignore directories
		return 0;
	}
	
	my $file_filename = $_.""; #for adding a 1 or something to the filename so we can test more with fewer files
	my $file_path = $File::Find::dir;
	
	my $local_file_state_corrected = "false"; # V.10: added to catch file state problems
	
	# *** Local state and server state are separate to reduce the number of server hits. we only really need to check the server status once. otherwise, we can keep track internally
	
	my $server_file_state = getStatus($file_filename, $file_path);
	# next line from: http://stackoverflow.com/questions/1915746/in-perl-how-can-i-find-the-index-of-a-given-value-in-an-array
	my $local_file_state = first { $file_states[$_] eq $server_file_state } 0..$#file_states;
	if (!defined($local_file_state)) {  
		log_message ("\n\n***STATE-ERROR: [".$file_filename."] Undefined server state (".$server_file_state.")***\n");
		exit (1);
	}
	
	debug_message("[file state report][".$file_filename."] Local state (from the server)".$local_file_state);
	if ($local_file_state eq "0") {
	    #set to CWRC-process-start 
	    $local_file_state++;
		if (setStatus($file_filename, $file_path, $file_states[$local_file_state]) eq "false") { # dual function: sets state, checks for errors
			log_message ("[file state change][".$file_filename."] state was successfully changed to ".$file_states[$local_file_state]."\n");
		}
		else {
			# in this situation, we have caught a collision (ie the state was changed without us knowing it)
			log_message ("\n\n***STATE-ERROR: [".$file_filename."] error caught while trying to change state to".$file_states[$local_file_state]."***\n");
			# skip this file
			next;
		}
	}
	
	if ($local_file_state eq "2" || $local_file_state eq "4") {
	# if we are out here and you have one of these states it means you were interrupted
	# so, we subtract one from the state, to get it to re-run the failed state
	
		$local_file_state--;
		log_message ("[file state change][".$file_filename."] File state error detected (interrupted process detected) state was successfully changed (back) to ".$file_states[$local_file_state]."\n");
		$local_file_state_corrected = "true"; # V.10: added to catch file state errors
	
	}
	
	if ($local_file_state eq "1") { # ready to convert pngs
		# set to CWRC-createpng-start
		$local_file_state++;
		debug_message("[file state report][".$file_filename."] Local state, inside the CWRC-createpng if: ".$local_file_state);
		if ($local_file_state_corrected eq "true" || setStatus($file_filename, $file_path, $file_states[$local_file_state]) eq "false") { # V.10: added to catch file state errors, See Note #1
			log_message ("[file state change][".$file_filename."] state was successfully changed to ".$file_states[$local_file_state]."\n");
			# do the conversion!
			my $file_command = "convert ".$file_path."/".$file_filename." -thumbnail '2000x2000>' ".
							"\\( +clone -resize '100x100>' -write ".$file_path."/".$file_filename.".thumb.png +delete \\) ".
							"\\( +clone -resize '500x500>' -write ".$file_path."/".$file_filename.".gallery.png +delete \\) ".
							$file_path."/".$file_filename.".detail.png";
			debug_message("[command][".$file_filename."] In CWRC-createpng: the convert command: ".$file_command."\n");
		    my $file_result = `$file_command  2>&1`;
		    debug_message("[command output][".$file_filename."] In CWRC-createpng: the convert command output: ".$file_result."\n");
		    # http://stackoverflow.com/questions/777543/how-can-i-read-the-error-output-of-external-commands-in-perl
		    my $file_errorReturn = "FAIL" if $?;
		    if (defined($file_errorReturn)) {
		    	log_message ("\n\n***COMMAND-ERROR: [".$file_filename."] error caught while trying to convert this file. Skip this file. Error code returned by the system: ".$?."***\n\n"); # V.11: changed $file_errorReturn to $? so that we get the actual error and not just "FAIL"
		    	$file_error_in_bag = "true"; # V.11: so we failed to create the pngs for this file THAT means, this bag should not be converted to CWRC-pngbag-end
		    	next;
		    }
		    
		    #set to CWRC-createpng-start
	    	$local_file_state++;
			setStatus($file_filename, $file_path, $file_states[$local_file_state]);
			log_message ("[file state change][".$file_filename."] state was successfully changed to ".$file_states[$local_file_state]."\n");
		}
		else {
			# in this situation, we have caught a collision (ie the state was changed without us knowing it)
			log_message ("\n\n***STATE-ERROR: [".$file_filename."] error caught while trying to change state to".$file_states[$local_file_state]."***\n\n");
			# skip this file
			next;
		}
	}
	
	if ($local_file_state eq "3") { # pngs created, ready to upload
		# set to CWRC-upload-start
		$local_file_state++;
		debug_message("[file state report][".$file_filename."] Local state, inside the CWRC-upload if: ".$local_file_state);
		if ($local_file_state_corrected eq "true" || setStatus($file_filename, $file_path, $file_states[$local_file_state]) eq "false") { # V.10: added to catch file state errors, See Note #1
			log_message ("[file state change][".$file_filename."] state was successfully changed to ".$file_states[$local_file_state]."\n");
			
			# -- all --
			sleep (1); # so as not to hammer the server
			
			my $upload_command = "nice /usr/bin/rsync -qogt --partial --partial-dir=".$destination_path."partials ".$file_path."/*.png ". $destination_user."@".$destination_hostname.":".$destination_path;
			debug_message("[command][".$file_filename."] In CWRC-upload: Upload command: ".$upload_command."\n");
			my $upload_result = `$upload_command  2>&1`;
			debug_message("[command output][".$file_filename."] In CWRC-upload: Upload command output: ".$upload_result."\n");
			my $file_errorReturn = "FAIL" if $?;
			if (defined($file_errorReturn)) {
			    log_message ("\n\n***COMMAND-ERROR: [".$file_filename."] error caught while trying to upload this file (thumb). Skip this file. Error code returned by the system: ".$?."***\n\n");
			    $file_error_in_bag = "true"; # V.11: so we failed to create the pngs for this file THAT means, this bag should not be converted to CWRC-pngbag-end
			    next;
			}    
					    			
			#set to CWRC-upload-end
	    	$local_file_state++;
			setStatus($file_filename, $file_path, $file_states[$local_file_state]);
			log_message ("[file state change][".$file_filename."] state was successfully changed to ".$file_states[$local_file_state]."\n");
		}
		else {
			# in this situation, we have caught a collision (ie the state was changed without us knowing it)
			log_message ("\n\n***STATE-ERROR: [".$file_filename."] error caught while trying to change state to".$file_states[$local_file_state]."***\n\n");
			# somehow skip this file
			next;
		}
	}
	
	if ($local_file_state eq "5") { # no changes required as this was the last step
		# set to CWRC-process-end
		$local_file_state++;
		setStatus($file_filename, $file_path, $file_states[$local_file_state]);
		log_message ("[file state change][".$file_filename."] state was successfully changed to ".$file_states[$local_file_state]."\n\n");
	}
		
} # end png_that_bag


# NOTE #1: This little bit of code
# if ($local_bag_state_corrected eq "true" || setStatus("", $bag_id, $bag_states[$local_bag_state]) eq "false") {
	
# basically says, if the bag state was adjusted, no need to tell the server.
# the second part of the || doesn't get evaluated, so on an adjusted state, the server
# already knows what the current value is, so we get a true on the left of the || and the
# right is never evaluated and the server isn't re-notified: win!










