#! perl -w

package CDBUtil;

use strict;
use DBI;
use Config::Simple;
use feature qw(say);

CDBUtil->new();

sub new {
	my $proto = shift;
	my $class = ref($proto) || $proto;
	my %args  = @_;

	my $cfg = new Config::Simple('CDBUtil.ini')
		or die Config::Simple->error();
	my $cdb = $cfg->get_block('CDB');
	my $qad = $cfg->get_block('QAD');

	my $self = { CDB_dbh => $args{CDB_dbh} || init_handle($cdb),
				 QAD_dbh => $args{QAD_dbh} || init_handle($qad),
				 %args
	};

	bless $self, $class;

	return $self;
}

sub gen_deletes {
	my $self = shift;
	my ( $start_date, $start_num, $table_selected ) = @_;

	unless ( $start_date && $start_num && $table_selected ) {
		warn
			'must supply start_date, start_num and table_selected in CDB UPD gen';
		return;
	}
	my ( $dbh_cdb, $dbh_qad ) = ( $self->{CDB_dbh}, $self->{QAD_dbh} );

	# retreive all tables to be regenerated from qad
	# change to retrieve specific table groups
	my $tables_aref = $dbh_qad->selectall_arrayref( "
		select name 
		from sys.tables 
		where 
		name = '$table_selected' 
		--and name not like '%_changes'" );

	my %tables_keys;
	my %tables_cols;
	for my $table_aref (@$tables_aref) {
		my ($table_name) = @$table_aref;

		# get all table primary keys
		my $table_keys_aref = $dbh_qad->selectall_arrayref( "
			select distinct column_name, ordinal_position
			from information_schema.key_column_usage
			where objectproperty(object_id(constraint_name), 'IsPrimaryKey') = 1
			and table_name = '$table_name'
			order by ordinal_position asc
		" );

		# get all column data types
		my $table_types_aref = $dbh_qad->selectall_arrayref( "
			select column_name, data_type, ordinal_position
			from information_schema.columns
			where table_name = '$table_name'
			order by ordinal_position asc
		" );

		# build hash of key types
		my %key_types;
		for my $type_aref (@$table_types_aref) {
			my ( $key, $type, $position ) = @$type_aref;
			$key_types{$key} = $type;
		}

		# build hash of table's actual keys
		# also, convert datetime columns to Pervasive Julian numbers
		for my $key_aref (@$table_keys_aref) {
			my ( $key, $position ) = @$key_aref;
			print "$table_name $key $key_types{$key}\n";
			if ( $key_types{$key} =~ m/datetime/i ) {
				$key = "(CONVERT([int],$key,(0))+(2415021))";
			}
			push @{ $tables_keys{$table_name} }, $key;
		}

		# total column count
		my $tables_cols_aref = $dbh_qad->selectall_arrayref( "
			select count(*)
			from information_schema.columns
			where table_name = '$table_name'
		" );

		# odd way of saving number of columns in table to hash...
		for my $col_aref (@$tables_cols_aref) {
			my ($col_count) = @$col_aref;

			#print "col count for $table_name = $col_count\n";
			$tables_cols{$table_name} = $col_count;
		}

	}

	# create UPDs from change database from distinct keys
	for my $table ( keys %tables_keys ) {
		print "compiling UPD for $table...\n";
		open( UPD, '>', "$table.UPD" );
		my $keys_select = join ', ', @{ $tables_keys{$table} };
		my $num_keys    = scalar @{ $tables_keys{$table} };
		my $num_tabs    = $tables_cols{$table} - $num_keys;

		#print "$num_tabs $tables_cols{$table} $num_keys\n";

		# query to select non-deletes since date/filenum range
		# TODO: need to change to reverse deletes as well
		my $query = "select distinct $keys_select
			from $table
			where 
			(FileDate_ > $start_date
			or (FileDate_ = $start_date and FileNum_ >= $start_num))
			and UpdateFlag_ != 'D'
		";

		#print "$query\n";

		my $delete_rows_aref = $dbh_cdb->selectall_arrayref($query);

		for my $delete_aref (@$delete_rows_aref) {
			my @deletes = @$delete_aref;
			print UPD "D\t", join( "\t", @deletes ), "\t" x $num_tabs, "\n";
		}

		close UPD;
	}
}

sub gen_updates {

}

sub list_keys {

}

sub last_upd {

}

sub init_handle {
	my $db = shift;

	# connecting to master since database may need to be created
	return
		DBI->connect(
		sprintf(
			"dbi:ODBC:Database=%s;Driver={SQL Server};Server=%s;UID=%s;PWD=%s",
			$db->{name} || 'master', $db->{server},
			$db->{user}, $db->{pwd}
		)
		) or die "failed to initialize database handle\n", $DBI::errstr;
}

=pod

=head1 NAME

CDBUtil - a package for interfacing with and extracting/formatting data from the ChangeDB

=head1 SYNOPSIS

	use CDBUtil;
	my $cdb = CDBUtil->new();

=head1 AUTHOR

Matt Shockley

=head1 COPYRIGHT AND LICENSE
Copyright 2012 Matt Shockley

This program is free software; you can redistribute it and/or modify 
it under the same terms as Perl itself.

=cut
