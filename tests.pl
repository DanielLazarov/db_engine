use warnings;
use strict;

use Data::Dumper;
use Time::HiRes qw( time );

use Math::BigInt;
use DanielDB::Engine;


#DanielDB::Engine::CreateDB("db_2");
#DanielDB::Engine::CreateDB("db_1");
my $db = DanielDB::Engine->connect("db_1");

sub FiveIntColumnsInsert($$)#Rows inserted: 100000, Time:5.08060479164124s.
{
    my ($db, $insert_row_count) = @_;

    my @columns = (
        {type => "int", name => "column1"}, 
        {type => "int", name => "column2"},
        {type => "int", name => "column3"},
        {type => "int", name => "column4"},
        {type => "int", name => "column5"},
    );
 #   $db->CreateTable("int_table", \@columns);

    my $data = {
        column1 => 123451,
        column2 => 123456,
        column3 => 1234567,
        column4 => 12345678,
        column5 => 123456789
    };

    my $start = time();
    for(my $i = 0; $i < $insert_row_count; $i++)
    {
        $db->Insert("int_table", $data);
    }
    print "Rows inserted: " . $insert_row_count . "\n";
    print "Time:",  time() - $start , "s.\n";
}



sub MixedBigColumnsInsert($$)#Rows inserted: 100000, Time :183.23756480217s.
{
    my($db, $insert_row_count) = @_;

    my @columns = (
        {type => "int", name => "column1"}, 
        {type => "int", name => "column2"},
        {type => "int", name => "column3"},
        {type => "int", name => "column4"},
        {type => "int", name => "column5"},
        {type => "text", name => "column6"},
        {type => "text", name => "column7"},
        {type => "text", name => "column8"},
    );
    $db->CreateTable("mixed_table", \@columns);

    my $data = {
        column1 => 1,
        column2 => 123456,
        column3 => 1234567,
        column4 => 12345678,
        column5 => 123456789,
        column6 => "TEST1",
        column7 => "TEST1",
        column8 => "TEST1",
    };

    my $start = time();
    for(my $i = 0; $i < $insert_row_count; $i++)
    {
        $$data{column1} = rand(100);
        $db->Insert("mixed_table", $data);
    }

    print "Rows inserted: " . $insert_row_count . "\n";
    print "Time:",  time() - $start , "s.\n";
}

sub FiveIntColumnsUpdate($)
{
    my ($db) = @_;

    my $data = {
        column1 => 12345,
        column2 => 123456,
        column3 => 1234567,
        column4 => 12345678,
        column5 => 123456789
    };

    my $start = time();

    $db->Update("int_table", $data);

    print "Time:",  time() - $start , "s.\n";
}

sub MixedBigColumnsUpdate($)
{
    my($db) = @_;

    my $data = {
        column1 => 123451,
        column2 => 123456,
        column3 => 1234567,
        column4 => 12345678,
        column5 => 123456789,
        column6 => "col6valueNEW1",
        column7 => "col7valueNEW2",
        column8 => "col8valueNEW3",
        
    };

    my $start = time();

    $db->Update("mixed_table", $data);

    print "Update Time:",  time() - $start , "s.\n";  
}

sub MixedBigColumnsRead($)
{
    my($db) = @_;


    my $start = time();

    my $result = $db->Select("mixed_table", {column1 =>{op => "==", val => 95}});

    print Dumper $result;

    print "Read Time:",  time() - $start , "s.\n";  
}

sub MixedBigColumnsDelete($)
{
    my($db) = @_;


    my $start = time();

    $db->DeleteRecord("mixed_table");

    print "Delete Time:",  time() - $start , "s.\n";  
}
#####################RUN TESTS##########################################################

#FiveIntColumnsInsert($db, 2);

#FiveIntColumnsUpdate($db);
#MixedBigColumnsInsert($db, 1000);
#MixedBigColumnsRead($db);
#MixedBigColumnsUpdate($db);
#MixedBigColumnsRead($db);
#MixedBigColumnsDelete($db);

#$db->CreateIndex("int_table", "column1");

#my $indexed = DanielDB::Engine::GetIndexedPositionsByValue(12345, "/var/daniel_db/db_1/int_table_column1_index");
#print Dumper $indexed;
#my $result = $db->GetIndexedPositions("mixed_table", "column1", 95);
#print Dumper $result;
#$db->ReadIndex("mixed_table", "column1");

    


##################################################################################
my $result = $db->Select("int_table");
print Dumper $result;

#$db->Update("int_table", {column1 => 99999}, {column1 => {val => 12345, op => "=="}});
#$result = $db->Select("int_table", {column1 => {val => 99999, op => "=="}});
#print Dumper $result;
$db->DeleteRecord("int_table", {column1 => {val => 123451, op => "=="}});

$result = $db->Select("int_table");
print Dumper $result;



#$data = {"column2" => "UPDATED"};
#$db->Update("table5", $data);

#$result = $db->Select("table5");
#print Dumper $result;
