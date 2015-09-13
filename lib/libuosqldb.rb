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
        puts "data: #{data}"

        # next 8 bytes mean the number of columns
        columns_num = @tcp.recv(8)
        num = byte_array_to_int(columns_num.bytes.to_a)
        puts "num of columns: #{num}"
        # subsequent columns
        col_arr = Array.new
        for i in 0...num
            # size of name string
            name_size = @tcp.recv(8)
            size = byte_array_to_int(name_size.bytes.to_a)
            # name string
            name = @tcp.recv(size)
            puts "col: #{i}\nname: #{name}"

            # SqlType 4 bytes for Int and Bool, Char has an additional 
            # following byte, VarChar has two additional following bytes
            sql_type = @tcp.recv(4)
            sql_type = sql_type.bytes.to_a.pack("C*").unpack("N").first
            if sql_type == 0 # Int
                puts "SqlType::Int"
            elsif sql_type == 1 # Bool
                puts "SqlType::Bool"
            elsif sql_type == 2 # Char(u8)
                size = @tcp.recv(1).bytes.to_a.first
                puts "SqlType::Char(#{size})"
            elsif sql_type == 3 # VarChar(u16)
                size = @tcp.recv(2).bytes.to_a.pack("C*").unpack("n").first
                puts "SqlType::VarChar(#{size})"
            end
            # one byte for is_primary_key  
            is_prim = @tcp.recv(1).bytes.to_a.first
            if is_prim == 0 # false
                puts "is_prim: false"
            else
                puts "is_prim: true"
            end

            # one byte for allow_null
            allow_null = @tcp.recv(1).bytes.to_a.first
            if allow_null == 0 # false
                puts "is_prim: false"
            else
                puts "is_prim: true"
            end

            # 8 bytes for the description string size
            descr_size = @tcp.recv(8)
            size = byte_array_to_int(descr_size.bytes.to_a)
            # name string
            descr = @tcp.recv(size)
            puts "descr: #{descr}"

            # add column to the array
            column = Column.new name, sql_type,is_prim, allow_null, descr
            col_arr << column
        end

        ResultSet.new data, col_arr
    end
end

class ResultSet

    def initialize (data, columns)
        @data = data
        @columns = columns
    end
end

class Column 

    def initialize (name, sql_type, is_primary_key, allow_null, description)
        @name = name
        @sql_type = sql_type
        @is_primary_key = is_primary_key
        @allow_null = allow_null
        @description = description
    end
end

##################### Tests ##########################################

=begin
=end
conn = connect("127.0.0.1", 4242, "con", "nect")
if conn.class == Connection
    puts "Versionsnr: #{conn.get_version}. Authenticated as #{conn.get_username}"
    puts "#{conn.get_greeting_msg}"
    
    if conn.ping 
        puts "ping done"
    else 
        puts "ping failed"
    end
    
    results = conn.execute "select * from test"
    if results === nil
        puts "execute failed"
    else
        puts "Received results"
    end
    if conn.quit
        puts "quit successful."
    else
        puts "quit failed."
    end

else
    puts "connection failed"
end

