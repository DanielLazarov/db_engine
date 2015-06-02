package DanielDB::Engine;

use strict;

use bytes;

use Config;
use Data::Dumper;
#use File::Sync qw(fsync sync);
use Math::BigInt;


=pod
Daniel DB.
=cut

my $DDB_ROOT_DIR = "/var/daniel_db/";

our $type_map = {
    "int" => 1,
    "text" => 2
};

our $sub_map = {
    1 => {read => \&ReadInt, write => \&WriteInt},
    2 => {read => \&ReadString, write => \&WriteString}
};

sub connect($$)
{
	my ($class, $db) = @_;

    my $err_log;

    if(!defined $db)
    {
        return bless {}, $class;
    }
    elsif( -d "$DDB_ROOT_DIR" . $db)
    {
        return bless {db_dir => $DDB_ROOT_DIR . $db}, $class;   
    }
    else
    {
        die "Not existing database: $db\n";
    }
}
sub BigIntUnpack($)
{   
    
    my ($int1,$int2)=unpack('NN',shift());
    my $sign=($int1&0x80000000);
    if($sign)
    {
        $int1^=-1;
        $int2^=-1;
        ++$int2;
        $int2%=2**32;
        ++$int1 unless $int2;
    }
    
    my $i=new Math::BigInt $int1;
    $i*=2**32;
    $i+=$int2;
    $i=-$i if $sign;
    return $i;
}

sub BigIntPack($)
{   
    my $i = new Math::BigInt shift();
    my($int1,$int2)=do {
        if ($i<0) {
            $i=-1-$i;
            (~(int($i/2**32)%2**32),~int($i%2**32));
        } else {
            (int($i/2**32)%2**32,int($i%2**32));
        }
    };
    my $packed = pack('NN',$int1,$int2);
    return $packed;
}

sub CreateDB($)
{
    my ($db_name) = @_;

    mkdir($DDB_ROOT_DIR . $db_name) or die $!;
}

sub CreateTable($$$)
{
    my ($self, $table_name, $columns) = @_;
    
    if(-f $$self{db_dir} . "/$table_name")
    {
        die "Table already exists";
    }

    my $fh;
    open($fh, ">" . $$self{db_dir} . "/$table_name") or die   $$self{db_dir} . "/$table_name" .$!;

    my @columns_arr = @{$columns};

    #print $fh pack("I", 0);
    print $fh pack("C", scalar @columns_arr); #column count

    foreach my $column(@columns_arr)
    {
        print $fh pack("C", $$type_map{$$column{type}});
        print $fh pack("C", length($$column{name}));
        print $fh $$column{name};
    }
    close($fh);
}

sub ReadTableMeta($)
{
    my ($fh) = @_;

    my @col_arr;
    
    seek $fh,0,0; #set fh at beginning

    my $buffer = "";

    read($fh, $buffer, 1);
    my $col_count = unpack("C", $buffer);
    
    for(my $i = 0; $i < $col_count; $i++)
    {
        my $column;

        read($fh, $buffer, 1);
        $$column{type} = unpack("C", $buffer);
        $$column{read} = $$sub_map{$$column{type}}{read};
        $$column{write} = $$sub_map{$$column{type}}{write};
     
        read($fh, $buffer, 1);
        my $colname_length = unpack("C", $buffer); #in bytes
        read($fh, $buffer, $colname_length);
        $$column{name} = $buffer;

        push @col_arr, $column;
    }
    return $fh, \@col_arr;
}

sub ReadRow($$)
{
    my($fh, $arr_of_handlers_ref) = @_;

    my @arr_of_handlers = @{$arr_of_handlers_ref};

    my @row;
    foreach my $handler(@arr_of_handlers)
    {
        push @row, $handler->($fh);
    }

    return $fh, \@row;
}

sub WriteRow($$$$)
{
    my($fh, $arr_of_handlers_ref, $arr_of_values_ref, $row_meta) = @_;

    my @arr_of_handlers = @{$arr_of_handlers_ref};
    my @arr_of_values = @{$arr_of_values_ref};

    my $length = scalar @arr_of_handlers;
    
    $fh = WriteRowMeta($fh, $row_meta);
    for(my $i = 0; $i < $length; $i++)
    {
        $arr_of_handlers[$i]->($fh, $arr_of_values[$i]);
    }

    return $fh;
}

=pod
sub WriteRow1($$$$)
{
    my($table_name, $arr_of_handlers_ref, $arr_of_values_ref, $row_meta) = @_;

    my $fh;
    open($fh, "+<", $table_name);
    flock($fh, 2);
    seek($fh,0,2);

    my @arr_of_handlers = @{$arr_of_handlers_ref};
    my @arr_of_values = @{$arr_of_values_ref};
    
    my $length = scalar @arr_of_handlers;
    
    my $row_meta_b = {busy => 1};
    
    my $pos = tell $fh; 
    $fh = WriteRowMeta($fh, $row_meta_b);

    for(my $i = 0; $i < $length; $i++)
    {
        $arr_of_handlers[$i]->($fh, $arr_of_values[$i]);
    }

    seek($fh, $pos, 0);
    $fh = WriteRowMeta($fh, $row_meta);

    close($fh);
}
=cut

sub Select($$;$)
{
    my ($self, $table_name, $conditions_href) = @_;

    my $fh;
    if(!-f $$self{db_dir} . "/$table_name")
    {
        die "Not Existing Table";
    }
    open($fh, "<", $$self{db_dir} . "/$table_name") or die $!;
    
    my $last_pos = (stat($fh))[7];

    my ($fh, $arr_ref) = ReadTableMeta($fh);

    my @columns_arr = @{$arr_ref};
    my @colnames;
    my @handlers;
    my @result;

    foreach my $column(@columns_arr) #get Colnames
    {
        push @colnames, $$column{name};
        push @handlers, $$column{read};
    }
    push @result, \@colnames;

    my $use_index;
    my $index_column;
    if(defined $conditions_href)
    {
        foreach my $key(keys %$conditions_href)
        {
            if(-f $$self{db_dir}. "/$table_name" . "_" . $key . "_index" && $$conditions_href{$key}{op} eq "==")
            {
                $use_index = 1;
                $index_column = $key;
                last;
            }
        }
    }

    if($use_index)
    {
        #TODO not like that
        my @positions = @{GetIndexedPositionsByValue($$conditions_href{$index_column}{val}, $$self{db_dir}. "/$table_name" . "_" . $index_column . "_index")};

        foreach my $position(@positions)
        {
            seek($fh, $position, 0);
            
            my $row_flags;
            ($fh, $row_flags) = ReadRowMeta($fh);
            if($$row_flags{busy})
            {
                last;
            }
            my $row;
            ($fh, $row) = ReadRow($fh, \@handlers);

            my $is_valid = CheckCondition($row, $conditions_href, \@columns_arr);
        
        
            if($is_valid && !$$row_flags{deleted})
            {
                push @result, $row;
            }

        }
    }
    else
    {
        while(tell($fh) < $last_pos)
        {
            my $row_flags;
            ($fh, $row_flags) = ReadRowMeta($fh);
            if($$row_flags{busy})
            {
                last;
            }
            my $row;
            ($fh, $row) = ReadRow($fh, \@handlers);

            my $is_valid = CheckCondition($row, $conditions_href, \@columns_arr);
        
        
            if($is_valid && !$$row_flags{deleted})
            {
                push @result, $row;
            }
        }

    }

    close($fh);

    return \@result;
}

sub Insert($$$)#TODO insert_hash may be arr_ref(bulk)
{
    my ($self, $table_name, $insert_hash) = @_;

    my $fh;
    if(!-f $$self{db_dir}. "/$table_name")
    {
        die "Not Existing Table";
    }
    open($fh, "+<" . $$self{db_dir} . "/$table_name") or die $!;
    
    my $arr_ref;

    ($fh, $arr_ref) = ReadTableMeta($fh);
   
    my @columns_arr = @{$arr_ref};

    my $col_count = scalar @columns_arr;
    seek($fh, 0, 2);
   
    my $row_meta_b = {busy => 1};
    my $pos = tell $fh;
    $fh = WriteRowMeta($fh, $row_meta_b);
    foreach my $column(@columns_arr)
    {
        $$column{write}->($fh, $$insert_hash{$$column{name}});
        if(-f $$self{db_dir}. "/$table_name" . "_" . $$column{name} . "_index")
        {
            my @index_arr;
            push @index_arr, {pos => $pos, value => $$insert_hash{$$column{name}}};
            InsertIndex(\@index_arr, $$self{db_dir}. "/$table_name" . "_" . $$column{name} . "_index");
        }
    }
    seek($fh,$pos,0);
    $fh = WriteRowMeta($fh);
    close($fh);
}


sub Update($$$;$)
{
    my ($self, $table_name, $update_hash, $conditions_href) = @_;

    my $fh;

    if(!-f $$self{db_dir} . "/$table_name")
    {
        die "Not Existing Table";
    }
    open($fh, "+<", $$self{db_dir} . "/$table_name") or die $!; 
    
    my $last_pos = (stat($fh))[7];
 
    my $arr_ref;
    ($fh, $arr_ref) = ReadTableMeta($fh);
    
    my @columns_arr = @{$arr_ref};
    my @handlers_read;
    my @handlers_write;
    my @update_arr;
    my @condition_arr;

    foreach my $column(@columns_arr) #get Colnames
    {
        push @handlers_read, $$column{read};
        push @handlers_write, $$column{write};
        push @condition_arr, $$conditions_href{$$column{name}};
    }

    while(tell($fh) < $last_pos)
    {
        my $beginning_row_pos = tell($fh);
        my ($fh, $row_flags) = ReadRowMeta($fh);
        if($$row_flags{busy})
        {
            last;
        }
        my ($fh, $row_ref) = ReadRow($fh, \@handlers_read);

        my @row = @{$row_ref};
        my $cols_count = scalar(@columns_arr);

        my $is_valid = CheckCondition(\@row, $conditions_href, \@columns_arr);
        if($is_valid && !$$row_flags{deleted})
        {
            my $next_row_pos = tell($fh);
            seek($fh, $beginning_row_pos, 0);
            $fh = WriteRowMeta($fh, {deleted => 1});
            
            my $update_row;
            
            seek($fh, 0, 2);
            my $pos = tell($fh);
            $fh = WriteRowMeta($fh, {busy => 1});
            for(my $i = 0; $i < $cols_count; $i++)
            {
                my $column = $columns_arr[$i];
                
                if(exists $$update_hash{$$column{name}})
                {
                    $$update_row{$$column{name}} = $$update_hash{$$column{name}};
                }

                else
                {
                    $$update_row{$$column{name}} = $row[$i];
                }

                $$column{write}->($fh, $$update_row{$$column{name}});
                if(-f $$self{db_dir}. "/$table_name" . "_" . $$column{name} . "_index")
                {
                    my @index_arr;
                    push @index_arr, {pos => $pos, value => $$update_row{$$column{name}}};
                    InsertIndex(\@index_arr, $$self{db_dir}. "/$table_name" . "_" . $$column{name} . "_index");
                }
            }
            print "Inserted row: ", Dumper $update_row;
            seek($fh,$pos,0);
            $fh = WriteRowMeta($fh);
            seek($fh, $next_row_pos, 0);
        }
    }

    close($fh);
}

sub DeleteRecord($$;$)
{
    my($self, $table_name, $conditions_href) = @_;     

    my $fh;
    my $arr_ref;

    open ($fh, "+<", $$self{db_dir} . "/" . $table_name);

    flock($fh, 2);
    seek($fh,0,2);
    my $last_pos = tell($fh);
    seek($fh,0,0);

    ($fh, $arr_ref) = ReadTableMeta($fh);
    
    my @columns_arr = @{$arr_ref};
    my @handlers;
    my @result;

    foreach my $column(@columns_arr) #get Colnames
    {
        push @handlers, $$column{read};
    }
#TODO Add indexed search
    while(tell($fh) < $last_pos)
    {
        my $row_beginning = tell($fh);

        my $row_flags;
        my $row;
        ($fh, $row_flags) = ReadRowMeta($fh);
        ($fh, $row) = ReadRow($fh, \@handlers);

        my $is_valid = CheckCondition($row, $conditions_href, \@columns_arr); 
        if($is_valid && !$$row_flags{deleted})
        {
            my $row_ending = tell($fh);
            seek($fh, $row_beginning, 0);
            $fh = WriteRowMeta($fh, {deleted => 1});
            seek($fh, $row_ending, 0);
        }
    }

    close($fh);
}

sub CreateIndex($$$)
{
    my($self, $table_name, $column_name) = @_;

    if(!-f $$self{db_dir} . "/$table_name")
    {
        die "Table \'$table_name\' does not exist.\n";
    }
    if(-f $$self{db_dir} . "/$table_name" . "_" . $column_name . "_index")
    {
        die "Index on \'$column_name\' on table \'$table_name\' already exists.\n";
    }

    my $fh;
    my $fha;
    open($fh, "<", $$self{db_dir} . "/$table_name");

    my $last_pos = (stat($fh))[7];

    my $arr_ref;
    ($fh, $arr_ref) = ReadTableMeta($fh);
    
    my @columns = @{$arr_ref};
    my @handlers;

    my $column_index;

    my $col_count = scalar @columns;
    for(my $i = 0; $i < $col_count; $i++)
    {
        if($columns[$i]{name} eq $column_name)
        {
            $column_index = $i;
        }
        push @handlers, $columns[$i]{read};
    }

    my @index_arr;

    while(tell($fh) < $last_pos)
    {
        my $row_flags;
        my $row_ref;
        my $row_position = tell($fh);
        ($fh, $row_flags) = ReadRowMeta($fh);
        if($$row_flags{busy})
        {
            last;
        }
                
        ($fh, $row_ref) = ReadRow($fh, \@handlers);
        
        my @row = @{$row_ref};
        
        push @index_arr, {value => $row[$column_index], pos => $row_position};
    }
    close($fh);
    
    InsertIndex(\@index_arr, $$self{db_dir} . "/$table_name". "_" . $column_name . "_index");
    
    close $fh;
}

sub InsertIndex($$)
{
    my ($index_arr_ref, $filename) = @_;
    
     #TODO ASSERT exists filename

    my $fh;
    my $is_first = 0;
    if(! -f $filename)
    { 
        $is_first = 1;
        open($fh, ">", $filename ) or die $!;
        close($fh); 
    }
    open($fh, "+<", $filename) or die $!;

    foreach my $record(@{$index_arr_ref})
    {
        my $pos;
        seek($fh,0,2);
        $pos = tell($fh);
        
        print $fh pack("i", $$record{value});
        print $fh BigIntPack($$record{pos});
        print $fh BigIntPack(0); #lchild
        print $fh BigIntPack(0); #rchild
        
        if(!$is_first)
        {
            my $child_pos = GetIndexFreeChildPos($$record{value}, $filename);
            print "Child pos: $child_pos\n";
            seek($fh, $child_pos,0);
            print $fh BigIntPack($pos);
        }
        $is_first = 0;
    }
}

sub GetIndexFreeChildPos($$)
{
    my($value, $filename) = @_;

    my $fh;
    open($fh, "<", $filename);
    
    my $pos;
    while(1)
    {
        my $node; 
        ($fh, $node) = ReadIndexNode($fh);
        
        if($$node{value} > $value)
        {
            if(!$$node{lchild})
            {
                return $$node{lchild_pos};
            }
            else
            {
                seek($fh, $$node{lchild}, 0);
            }
        }
        else
        {
            if(!$$node{rchild})
            {
                return $$node{rchild_pos};
            }
            else
            {
                seek($fh, $$node{rchild}, 0);
            }
        }
        
    }
}

sub GetIndexedPositionsByValue($$;$)
{
    my($value, $filename, $op) = @_;
    
    my $fh;
    open($fh, "<", $filename) or die $!;

    my @positions;

    while(1)
    {
        my $node;
        ($fh, $node) = ReadIndexNode($fh);

        if($value < $$node{value})
        {
            if(!$$node{lchild})
            {
                return \@positions;
            }
            else
            {
                seek($fh, $$node{lchild}, 0);
            }
        }
        else
        {
            if($$node{value} == $value)
            {
                push @positions, $$node{pos};
            }
            if(!$$node{rchild})
            {
                return \@positions;
            }
            else
            {
                seek($fh, $$node{rchild}, 0);
            }
        }
    }
}



sub ReadIndexNode($)
{
    my ($fh) = @_;

    my $node;
    my $buffer = "";
    
    read($fh, $buffer, $Config{intsize});
    $$node{value} = unpack("i", $buffer);

    read($fh, $buffer, 8);
    $$node{pos} = BigIntUnpack($buffer); # Skip value pos.
   
    $$node{lchild_pos} = tell($fh);
    read($fh, $buffer, 8);
    $$node{lchild} = BigIntUnpack($buffer);

    $$node{rchild_pos} = tell($fh);
    read($fh, $buffer, 8);
    $$node{rchild} = BigIntUnpack($buffer);
   
    seek($fh, -28, 1);
    
    return($fh, $node);
}


sub CheckCondition($$$)
{
    my($row_arr_ref, $conditions_ref, $columns_arr_ref) = @_;

    my @row = @{$row_arr_ref};
    my @columns = @{$columns_arr_ref};
    my $cols_count = scalar @columns;
    
    my $result = 1;
    
    for(my $i = 0; $i < $cols_count; $i++)
    {
        if(exists $$conditions_ref{$columns[$i]{name}})
        {
            if($columns[$i]{type} == 1)#int
            {
                #check if equal TODO add more conditions
                if($$conditions_ref{$columns[$i]{name}}{op} eq "==" && $row[$i] != $$conditions_ref{$columns[$i]{name}}{val})
                {    
                    $result = 0;
                    last;
                }
                elsif($$conditions_ref{$columns[$i]{name}}{op} eq "!=" && $row[$i] == $$conditions_ref{$columns[$i]{name}}{val})
                {    
                    $result = 0;
                    last;
                }
                elsif($$conditions_ref{$columns[$i]{name}}{op} eq ">" && $row[$i] <= $$conditions_ref{$columns[$i]{name}}{val})
                {    
                    $result = 0;
                    last;
                }
                elsif($$conditions_ref{$columns[$i]{name}}{op} eq "<" && $row[$i] >= $$conditions_ref{$columns[$i]{name}}{val})
                {    
                    $result = 0;
                    last;
                }
                elsif($$conditions_ref{$columns[$i]{name}}{op} eq ">=" && $row[$i] < $$conditions_ref{$columns[$i]{name}}{val})
                {    
                    $result = 0;
                    last;
                }
                elsif($$conditions_ref{$columns[$i]{name}}{op} eq "<=" && $row[$i] > $$conditions_ref{$columns[$i]{name}}{val})
                {    
                    $result = 0;
                    last;
                }
            }
            elsif($columns[$i]{type} == 2)#text
            {
                if($$conditions_ref{$columns[$i]{name}}{op} eq "==" && $row[$i] ne $$conditions_ref{$columns[$i]{name}}{val})
                {    
                    $result = 0;
                    last;
                }
                if($$conditions_ref{$columns[$i]{name}}{op} eq "!=" && $row[$i] eq $$conditions_ref{$columns[$i]{name}}{val})
                {    
                    $result = 0;
                    last;
                }
                if($$conditions_ref{$columns[$i]{name}}{op} eq ">" && ($row[$i] lt $$conditions_ref{$columns[$i]{name}}{val} || $row[$i] eq $$conditions_ref{$columns[$i]{name}}{val} ))
                {    
                    $result = 0;
                    last;
                }
                if($$conditions_ref{$columns[$i]{name}}{op} eq "<" && ($row[$i] gt $$conditions_ref{$columns[$i]{name}}{val} || $row[$i] eq $$conditions_ref{$columns[$i]{name}}{val} ))
                {    
                    $result = 0;
                    last;
                }
                if($$conditions_ref{$columns[$i]{name}}{op} eq ">=" && $row[$i] lt $$conditions_ref{$columns[$i]{name}}{val})
                {    
                    $result = 0;
                    last;
                }
                if($$conditions_ref{$columns[$i]{name}}{op} eq "<=" && $row[$i] gt $$conditions_ref{$columns[$i]{name}}{val})
                {    
                    $result = 0;
                    last;
                }
            }
        }    
    }

    return $result;
}

sub WriteRowMeta($;$)
{
    my($fh, $params) = @_;

    my $flags = 0;

    if(defined $$params{deleted} && $$params{deleted})
    {
        $flags += 128;
    }
    if(defined $$params{busy} && $$params{busy})
    {
        $flags +=64;
    }

    print $fh pack("C",$flags );

    return $fh;
}

sub ReadRowMeta($)
{
    my($fh) = @_;

    my $result= {
        deleted => 0
    };

    my $buffer;
    read($fh, $buffer, 1);  
    my $flags = unpack("C", $buffer);
    
    if($flags & (1<<7))
    {
        $$result{deleted} = 1;
    }
    if($flags & (1<<6))
    { 
        $$result{busy} = 1;
    }
    return ($fh, $result);
}
sub ReadInt($)
{
    my ($fh) = @_;
    my $buffer = "";

    #info byte variables1
    my $is_null = 0;
   
    read($fh, $buffer, 1);
    my $flags = unpack("C", $buffer);

    read($fh, $buffer, $Config{intsize});

    if($flags & (1<<7))
    {
        return undef;
    }
    else
    {
        return unpack("i", $buffer);
    }
}

sub WriteInt($$)
{
    my ($fh, $value) = @_;

    my $flags = 0;

    if(!defined $value)
    {
        $flags += 128;
    }
    
    print $fh pack("C", $flags);
    print $fh pack("i", $value);
}

sub ReadString($)
{
    my ($fh) = @_;
    
    my $buffer ="";

    my $is_null = 0;
    
    read($fh, $buffer, 1);
    my $flags = unpack("C", $buffer);

    read($fh, $buffer, $Config{intsize});
    my $str_length = unpack("I", $buffer);
    read($fh, $buffer, $str_length);

    if($flags & (1<<7))
    {
        return undef;
    }
    else
    {
        return $buffer;
    }
}

sub WriteString($$)
{
    my ($fh, $value) = @_;
    
    my $flags = 0;
    if(!defined $value)
    {
        $flags += 128;
    }
    
    print $fh pack("C", $flags);
    print $fh pack("I", bytes::length($value));
    print $fh $value;
}
1;
