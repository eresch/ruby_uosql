# lib for uosql database connection
require 'socket'

def connect (addr, portnr, username, password)
    """ static method to get a working instance of Connection"""
    begin
        Connection.new(addr, portnr, username, password)
    rescue => e
        puts e
        nil
    end
end

class Connection
    @@command = {quit: 0, ping: 1, query: 2}
    @@pkgtype = {greet: 0,
                 login: 1,
                 commands: 2,
                 error: 3,
                 ok: 4,
                 response: 5,
                 accdenied: 6,
                 accgranted: 7 }

    def initialize(addr, portnr, usern, passw)
        @login = {username: usern, password: passw}
        @ip_addr = addr
        @port = portnr
        connect()
    end

    private
    def connect()
        """ Connect to the given ip-address and port number """
        @tcp = TCPSocket.new @ip_addr, @port
        receive_greeting
        send_login
        receive_auth
    end

    private
    def receive_greeting
        """Receive the greeting package with version number and a
            message from the server """
        
        pkg = @tcp.recv(4)
        arr = pkg.bytes.to_a.pack("C*").unpack("N").first
        if arr != @@pkgtype[:greet]
            puts "unknown pkg, close connection, return nil"
            @tcp.close()
            return nil
        end

        # receive server versions number
        vrs_nr = @tcp.recv(1)
        
        # receive size of message
        pkg = @tcp.recv(8)
        num = byte_array_to_int(pkg.bytes.to_a)
        
        # receive message
        msg = @tcp.recv(num)
        
        # set the greeting message and version number
        @greet = {version: vrs_nr.bytes.to_a.first, message: msg}
    end

    private
    def send_login
        """ Send login package with username and password """
        @tcp.send([@@pkgtype[:login]].pack("N"), 0)
        @tcp.send([@login[:username].length].pack("Q>"), 0)
        @tcp.send(@login[:username], 0)
        @tcp.send([@login[:password].length].pack("Q>"), 0)
        @tcp.send(@login[:password], 0)
    end


    private
    def receive_auth
        """ Receive authentication status """
        pkg = @tcp.recv(4)
       
        arr = pkg.bytes.to_a.pack("C*").unpack("N").first
        if arr == @@pkgtype[:accgranted]
            # do nothing
        elsif arr == @@pkgtype[:accdenied]
            @tcp.close()
            @errormessage = {errorkind: @@pkgtype[:accdenied], 
                             message: "Access denied. Close connection"}
            raise "access denied" # throw a runtime exception
        elsif arr == @@pkgtype[:error]
            read_err
            raise "Error package received." # throw a runtime exception
        else
            @errormessage = {errorkind: 1, 
                             message: "Unexpected package received."}
            raise "Unexpected package received."
        end
    end

    private
    def read_err
        """ Read the package with errorkind and error message. """
        errkind = @tcp.recv(2) # ErrorKind
        pkg = @tcp.recv(8) # msgsize
        num = byte_array_to_int(pkg.bytes.to_a)
        msg = @tcp.recv(num)
        @errormessage = {errorkind: errkind, message: msg}
    end

    private
    def byte_array_to_int(length_array)
        """ Converts a four byte array to an integer value """
        len = 0
        for i in 0..7
            len += (length_array[7-i] * (256**i))
        end
        len
    end

    public
    def ping
        """ Send ping command. 
            Return true if ping was successful, else false. """
        begin
            @tcp.send([@@pkgtype[:commands]].pack("N"), 0)
            @tcp.send([@@command[:ping]].pack("N"), 0)
            pkg = @tcp.recv(4)
            arr = pkg.bytes.to_a.pack("C*").unpack("N").first # array of bytes : pkg type
            if arr == @@pkgtype[:ok]
                true
            elsif arr == @@pkgtype[:error]
                read_err
                false
            else 
                @errormessage = {errorkind: 1, 
                             message: "Unexpected package received."}
                false
            end
        rescue => e
            false
        end
    end

    public
    def quit
        """ Send quit command. Return true if quit was successful and 
            close the connection, else false. """
        begin 
            @tcp.send([@@pkgtype[:commands]].pack("N"), 0)
            @tcp.send([@@command[:quit]].pack("N"), 0)
            pkg = @tcp.recv(4)
            arr = pkg.bytes.to_a.pack("C*").unpack("N").first # array of bytes : pkg type
            if arr == @@pkgtype[:ok]
                @tcp.close
                true
            elsif arr == @@pkgtype[:error]
                read_err
                false
            else 
                @errormessage = {errorkind: 1, 
                             message: "Unexpected package received."}
                false
            end
        rescue => e
            false
        end
    end

    public
    def get_version
        """ Return the version of the server """
        @greet[:version]
    end

    public 
    def get_port
        """ Return the port number of the connection. """
        @port
    end

    public 
    def get_username
        """ Return the username used to authenticate this connection. """
        @login[:username]
    end

    public 
    def get_greeting_msg
        """ Return the greeting message sent by server. """
        @greet[:message]
    end

    public 
    def get_address
        """ Return the ip address of the connection """
        @ip_addr
    end

    public
    def execute(query)
        """ Send query to the server and receive response. Return ResultSet if
            query was successful, else nil. """
        begin
            # send the pkgtype
            @tcp.send([@@pkgtype[:commands]].pack("N"), 0)

            # send the query structure
            # four bytes for query command
            @tcp.send([@@command[:query]].pack("N"), 0)
            # size of query string
            @tcp.send([query.length].pack("Q>"), 0)
            # query string
            @tcp.send(query, 0)

            # response with result
            pkg = @tcp.recv(4)
       
            arr = pkg.bytes.to_a.pack("C*").unpack("N").first
            if arr == @@pkgtype[:response]
                # read the sent data
                read_data
            elsif arr == @@pkgtype[:error]
                read_err
                nil
            else
                @errormessage = {errorkind: 1, 
                                 message: "Unexpected package received."}
                nil
            end
        rescue => e
            nil
        end
    end

    private
    def read_data
        """ Receive Rows object. """
        # first 8 bytes mean the bytes number of data
        data_size = @tcp.recv(8)
        size = byte_array_to_int(data_size.bytes.to_a)
        # subsequent data
        data = @tcp.recv(size)
        data = data.bytes.to_a # convert to byte array

        # next 8 bytes mean the number of columns
        columns_num = @tcp.recv(8)
        num = byte_array_to_int(columns_num.bytes.to_a)
        # subsequent columns
        col_arr = Array.new
        for i in 0...num
            # size of name string
            name_size = @tcp.recv(8)
            size = byte_array_to_int(name_size.bytes.to_a)
            # name string
            name = @tcp.recv(size)

            # SqlType 4 bytes for Int and Bool, Char has an additional 
            # following byte, VarChar has two additional following bytes
            sql_type = @tcp.recv(4)
            sql_type = sql_type.bytes.to_a.pack("C*").unpack("N").first
            if sql_type == 0 # Int
                sql_type = Int.new
            elsif sql_type == 1 # Bool
                sql_type = Bool.new
            elsif sql_type == 2 # Char(u8)
                size = @tcp.recv(1).bytes.to_a.first
                sql_type = Char.new size
            elsif sql_type == 3 # VarChar(u16)
                size = @tcp.recv(2).bytes.to_a.pack("C*").unpack("n").first
                sql_type = VarChar.new size
            end
            # one byte for is_primary_key  
            is_prim = @tcp.recv(1).bytes.to_a.first
            if is_prim == 0 # false
                is_prim = false
            else
                is_prim = true
            end

            # one byte for allow_null
            allow_null = @tcp.recv(1).bytes.to_a.first
            if allow_null == 0 # false
                allow_null = false
            else
                allow_null = true
            end

            # 8 bytes for the description string size
            descr_size = @tcp.recv(8)
            size = byte_array_to_int(descr_size.bytes.to_a)
            # name string
            descr = @tcp.recv(size)

            # add column to the array
            column = Column.new name, sql_type,is_prim, allow_null, descr
            col_arr << column
        end

        ResultSet.new data, col_arr
    end

end
################################################################################
class ResultSet

    def initialize (data, columns)
        @metadata= MetaData.new columns         # array of Columns
        @current_line = -1                      # actual line position start = 0
        @line_size = @metadata.get_line_size    # size of line through all columns
        puts "#{data}"
        preprocess_data data
    end

    public
    def preprocess_data data
        col_count = @metadata.get_col_cnt                  # amount of columns
        @line_count = data.length / @metadata.get_line_size

        process_data = Array.new col_count

        for i in 0..(col_count -1)
            process_data[i] = Array.new @line_count
        end
        arr = Array.new
        for i in 0..(col_count -1)
            size = (@metadata.get_col_type i).size
            arr << size
        end

        pos = 0
        for i in 0..(@line_count -1)
            for j in 0..(col_count -1)
                column_size = arr[j]
                process_data[j][i] =  data[pos..(pos+column_size-1)]
                pos += (column_size)
            end
        end
        @data = process_data

    end

    public
    def get_col_cnt
        """ Return the number of columns in this ResultSet. """
        @metadata.get_col_cnt
    end

    public
    def get_col_name idx
        """ Return Column name at specified index, 
            nil if index is out of range. """
        @metadata.get_col_name idx
    end

    public
    def get_col_type idx
        """ Return Column type at specified index or with specified name,
            nil if index is out of range or name not in ResultSet. """
        @metadata.get_col_type idx
    end

    public 
    def get_col_idx name
        """ Return column idx with specified name, 
            nil if no column with specified name is in the ResultSet."""
        @metadata.get_col_idx name
    end

    public 
    def get_col_is_primary column
        """ Return boolean if values are primary keys in the column at specified
            index or with specified name, nil if no column with specified name 
            is in the ResultSet or index out of bounds. """
        @metadata.get_col_is_primary column
    end

    public
    def get_col_allow_null column
        """ Return boolean if null values are allowed in the column at specified
            index or with specified name, nil if no column with specified name 
            is in the ResultSet or index out of bounds. """
        @metadata.get_col_allow_null column
    end

    public 
    def get_col_description column
        """ Return description of column at specified index or with specified 
            name, nil if no column with specified name is in the ResultSet or
            index out of bounds. """
        @metadata.get_col_description column
    end

    public
    def nextInt column 
        if column.class == Fixnum
            if column >= @metadata.get_col_cnt || column < 0
                return nil
            else
                col = @metadata.get_col_type column
                if col.class != Int
                    return nil
                else
                    data = @data[column][@current_line]
                    if data.nil?
                        return nil
                    else
                        return data.pack("C*").unpack("N").first
                    end
                end
            end
        elsif column.class == String
            idx = get_col_idx column
            if idx.nil?
                return nil
            else
                return nextInt idx
            end
        end
        nil
    end


    public
    def nextBool column
        if column.class == Fixnum
            if column >= @metadata.get_col_cnt || column < 0
                return nil
            else
                col = @metadata.get_col_type column
                if col.class != Bool
                    return nil
                else
                    data = @data[column][@current_line]
                    if data.nil?
                        return nil
                    else
                        if data.first == 0
                            return false
                        else
                            return true
                        end
                    end
                end
            end
        elsif column.class == String
            idx = get_col_idx column
            if idx.nil?
                return nil
            else
                return nextBool idx
            end
        end
        nil
    end


    public 
    def nextChar column
        if column.class == Fixnum
            if column >= @metadata.get_col_cnt || column < 0
                return nil
            else
                col = @metadata.get_col_type column
                if col.class != Char
                    return nil
                else
                    data = @data[column][@current_line]
                    if data.nil?
                        return nil
                    else
                        return data.pack("c*")
                    end
                end
            end
        elsif column.class == String
            idx = get_col_idx column
            if idx.nil?
                return nil
            else
                return nextChar idx
            end
        end
        nil
    end

    public 
    def nextVarChar column
        if column.class == Fixnum
            if column >= @metadata.get_col_cnt || column < 0
                return nil
            else
                col = @metadata.get_col_type column
                if col.class != Char
                    return nil
                else
                    data = @data[column][@current_line]
                    if data.nil?
                        return nil
                    else
                        return data.pack("c*")
                    end
                    
                end
            end
        elsif column.class == String
            idx = get_col_idx column
            if idx.nil?
                return nil
            else
                return nextChar idx
            end
        end
        nil
    end

    public 
    def next
        if (@current_line + 1) == @line_count
            return false
        else
            @current_line += 1
            return true
        end
    end

    public 
    def previous
        if @current_line == 0
            return false
        else
            @current_line -= 1
            return true
        end
    end

end
################################################################################
class MetaData

    def initialize columns
        @columns = columns
    end

    public 
    def get_line_size
        line_size = 0
        @columns.each {|elem| line_size += (elem.get_sql_type).size}
        line_size
    end

    public
    def get_col_cnt
        """ Return number of columns. """
        @columns.length
    end

    public 
    def get_col_name idx
        """ Return Column name at specified index, nil if index is out of range. """
        if idx >= @columns.length || idx < 0
            nil
        else
            @columns[idx].get_name
        end
    end

    public 
    def get_col_idx name
        """ Return column with specified name, nil if no column with specified
            name is in the ResultSet."""
        @columns.each_index do |idx|
            if @columns[idx].get_name == name 
                return idx
            end
        end
        nil
    end

    public
    def get_col_type column
        """ Return Column type at specified index or with specified name,
            nil if index is out of range or name not in ResultSet. """
        if column.class == Fixnum
            if column >= @columns.length || column < 0
                nil
            else
                return @columns[column].get_sql_type
            end
        elsif column.class == String
            @columns.each do |elem|
                if elem.get_name == name
                    return elem.get_sql_type
                end
            end
            nil
        end
        nil
    end

    public 
    def get_col_is_primary column
        """ Return boolean if values are primary keys in the column at specified
            index or with specified name, nil if no column with specified name 
            is in the ResultSet or index out of bounds. """
        if column.class == Fixnum
            if column >= @columns.length || column < 0
                nil
            else
                return @columns[column].get_is_primary_key
            end
        elsif column.class == String
            @columns.each do |elem|
                if elem.get_name == column
                    return elem.get_is_primary_key
                end
            end
            nil
        end
        nil
    end

    public
    def get_col_allow_null column
        """ Return boolean if null values are allowed in the column at specified
            index or with specified name, nil if no column with specified name 
            is in the ResultSet or index out of bounds. """
        if column.class == Fixnum
            if column >= @columns.length || column < 0
                nil
            else
                return @columns[column].get_allow_null
            end
        elsif column.class == String
            @columns.each do |elem|
                if elem.get_name == column
                    return elem.get_allow_null
                end
            end
            nil
        end
        nil
    end

    public 
    def get_col_description column
        """ Return description of column at specified index or with specified 
            name, nil if no column with specified name is in the ResultSet or
            index out of bounds. """
        if column.class == Fixnum
            if column >= @columns.length || column < 0
                nil
            else
                return @columns[column].get_description
            end
        elsif column.class == String
            @columns.each do |elem|
                if elem.get_name == column
                    return elem.get_description
                end
            end
            nil
        end
        nil
    end
end
################################################################################
class Column 

    def initialize (name, sql_type, is_primary_key, allow_null, description)
        @name = name
        @sql_type = sql_type
        @is_primary_key = is_primary_key
        @allow_null = allow_null
        @description = description
    end

    public
    def get_name
        @name
    end

    public
    def get_sql_type
        @sql_type
    end

    public
    def get_is_primary_key
        @is_primary_key
    end

    public 
    def get_allow_null
        @allow_null
    end

    public 
    def get_description
        @description
    end
end
################################################################################
class SqlType

    public
    def size
        @size
    end
end
################################################################################
class Int < SqlType

    def initialize 
        @size = 4
    end

    public
    def to_s
        "Int"
    end
end

class Bool < SqlType

    def initialize
        @size = 1
    end

    public
    def to_s
        "Bool"
    end
end

class Char < SqlType

    def initialize size
        @size = size + 1
    end

    public
    def to_s
        "Char"
    end
end

class VarChar < SqlType

    def initialize size
        @size = size + 2
    end

    public
    def to_s
        "VarChar"
    end
end

##################### Tests ##########################################
conn = connect("127.0.0.1", 4242, "con", "nect")
if conn.class == Connection
    puts "Versionsnr: #{conn.get_version}. Authenticated as #{conn.get_username}"
    puts "#{conn.get_greeting_msg}\n\n"
    
    if conn.ping 
        puts "ping done\n\n"
    else 
        puts "ping failed\n\n"
    end
    
    results = conn.execute "select * from test"
    if results.nil?
        puts "execute failed\n\n"
    else
        puts "Received results!!!!!\n\n"
=begin
        puts "column name at 0: '#{results.get_col_name 0}'\n\n"
        
        idx = results.get_col_idx "error occurred"
        if  idx.nil?
            puts "Failed: no index for 'error occurred' found.\n\n"
        else
            puts "OK: column idx of 'error occurred': '#{idx}'\n\n"
        end

        is_prim = results.get_col_is_primary "error occurred"
        if is_prim.nil?
            puts "Failed: nil value for is_prim\n\n"
        else
            puts "OK: value is_primary_key: '#{is_prim}'\n\n"
        end

        is_prim = results.get_col_is_primary "err"
        if is_prim.nil?
            puts "OK: no column named 'err' in ResultSet.\n\n"
        else
           puts "Failed: get_col_is_primary_by_name failed\n\n"
        end

        allow_null = results.get_col_allow_null 0
        if allow_null.nil?
            puts "Failed: nil value for allow null\n\n"
        else
            puts "Ok: value allow null: '#{allow_null}'\n\n"
        end

        type = results.get_col_type 0
        if type.nil?
            puts "Failed: type nil\n\n"
        else
            puts "OK: Columntype: #{type}\n\n"
        end
=end
        while results.next
            puts " #{results.nextInt 0}, #{results.nextBool "something"}, #{results.nextChar 2}"
        end
    end
    

    if conn.quit
        puts "\nquit successful.\n\n"
    else
        puts "Failed: quit failed.\n\n"
    end

else
    puts "connection failed"
end

