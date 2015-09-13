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
            # send the pkgtype
            # @tcp.send([@@pkgtype[:commands]].pack("N"), 0)

            # send the query structure
    end
=begin
    private 
    def int_to_byte_array(length)
        """ Convert an integer value to an eight byte array """
        array = [0, 0, 0, 0, 0, 0, 0, 0] # default values
        for i in 0..7
            array[i] = length / 256**(7-i)
            length = length -array[i] * 256**(7-i)
        end
        array
    end
=end
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
    
    if conn.quit
        puts "quit successful."
    else
        puts "quit failed."
    end

else
    puts "connection failed"
end

