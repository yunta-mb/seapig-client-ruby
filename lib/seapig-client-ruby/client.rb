require 'websocket-eventmachine-client'
require 'json'
require 'jsondiff'
require 'hana'
require 'narray'


module WebSocket
	module Frame
		class Data < String
			def getbytes(start_index, count)
				data = self[start_index, count]
				if @masking_key
					payload_na = NArray.to_na(data,"byte")
					mask_na = NArray.to_na((@masking_key.pack("C*")*((data.size/4) + 1))[0...data.size],"byte")
					data = (mask_na ^ payload_na).to_s
				end
				data
			end
		end
	end
end



class SeapigClient

	attr_reader :uri, :socket, :connected, :error

	def initialize(uri, options={})
		@uri = uri
		@options = options
		@slave_objects = {}
		@master_objects = {}
		@connected = false
		@socket = nil
		@error = nil
		connect
	end


	def connect

		disconnect if @socket
		@reconnect_on_close = true

		@timeout_timer ||= EM.add_periodic_timer(10) {
			next if not @socket
			next if Time.new.to_f - @last_communication_at < 20
			puts "Seapig ping timeout, reconnecting" if @options[:debug]
			connect
		}

		@last_communication_at = Time.new.to_f

		puts 'Connecting to seapig server' if @options[:debug]
		@socket = WebSocket::EventMachine::Client.connect(uri: @uri)

		@socket.onopen {
			puts 'Connected to seapig server' if @options[:debug]
			@connected = true
			@error = nil
			@onstatuschange_proc.call(self) if @onstatuschange_proc
			@socket.send JSON.dump(action: 'client-options-set', options: @options)
			@slave_objects.each_pair { |id, object|
				@socket.send JSON.dump(action: 'object-consumer-register', pattern: id, :"version-known" => object.version)
				object.validate
			}
			@master_objects.each_pair { |id, object|
				@socket.send JSON.dump(action: 'object-producer-register', pattern: id, :"version-known" => object.version)
			}
			@last_communication_at = Time.new.to_f
		}

		@socket.onclose(&(@socket_onclose = Proc.new { |code, reason|
			puts 'Seapig connection closed (code:'+code.inspect+', reason:'+reason.inspect+')' if @options[:debug]
			@connected = false
			@socket = nil
			@timeout_timer.cancel if @timeout_timer
			@timeout_timer = nil
			@slave_objects.values.each { |object| object.invalidate }
			@onstatuschange_proc.call(self) if @onstatuschange_proc
			EM.cancel_timer(@reconnection_timer) if @reconnection_timer
			@reconnection_timer = nil
			@reconnection_timer = EM.add_timer(1) { connect } if @reconnect_on_close
		}))

		@socket.onerror { |error|
			puts 'Seapig socket error: '+error.inspect if @options[:debug]
			@error = { while: @socket ? "connecting" : "connected", error: error }
			@socket_onclose.call(nil, error) if @socket
		}

		@socket.onmessage { |data|
			message = JSON.load(data)
			case message['action']
			when 'object-update'
				@slave_objects.each_pair { |id, object|
					object.patch(message) if object.matches(message['id'])
				}
			when 'object-destroy'
				@slave_objects.each_pair { |id, object|
					object.destroy(message) if object.matches(message['id'])
				}
				@master_objects.each_pair { |id, object|
					object.destroy(message) if object.matches(message['id'])
				}
			when 'object-produce'
				handler = @master_objects.values.find { |object| object.matches(message['id']) }
				puts 'Seapig server submitted invalid "produce" request: '+message.inspect if (not handler) and @options[:debug]
				handler.produce(message['id'],message['version-inferred']) if handler
			else
				raise 'Seapig server submitted an unsupported message: '+message.inspect
			end
			@last_communication_at = Time.new.to_f
		}

		@socket.onping {
			@last_communication_at = Time.new.to_f
		}

	end


	def disconnect(detach_fd = false)
		@reconnect_on_close = false
		if @timeout_timer
			@timeout_timer.cancel
			@timeout_timer = nil
		end
		if @reconnection_timer
			EM.cancel_timer(@reconnection_timer)
			@reconnection_timer = nil
		end
		if @socket
			if detach_fd
				IO.new(@socket.detach).close
				@socket.onclose {}
				@socket_onclose.call("fd detach", "fd detach")
			else
				@socket.onclose {}
				@socket.close
				@socket_onclose.call("close","close")
			end
		end
	end


	def detach_fd
		disconnect(true)
	end


	def onstatuschange(&block)
		@onstatuschange_proc = block
		self
	end


	def slave(id, options={})
		raise "Both or none of 'object' and 'version' are needed" if (options[:object] and not options[:version]) or (not options[:object] and options[:version])
		@slave_objects[id] = if id.include?('*') then SeapigWildcardSlaveObject.new(self, id, options) else SeapigSlaveObject.new(self, id, options) end
		@socket.send JSON.dump(action: 'object-consumer-register', pattern: id, :"version-known" => @slave_objects[id].version) if @connected
		@slave_objects[id]
	end


	def master(id, options={})
		@master_objects[id] = if id.include?('*') then SeapigWildcardMasterObject.new(self, id, options) else SeapigMasterObject.new(self, id, options) end
		@socket.send JSON.dump(action: 'object-producer-register', pattern: id, :"version-known" => @master_objects[id].version) if @connected
		@master_objects[id]
	end


	def unlink(id)
		if @slave_objects[id]
			@slave_objects.delete(id)
			@socket.send(JSON.stringify(action: 'object-consumer-unregister', pattern: id)) if @connected
		end
		if @master_objects[id]
			@master_objects.delete(id)
			@socket.send(JSON.stringify(action: 'object-producer-unregister', pattern: id)) if @connected
		end
	end

end



class SeapigObject < Hash

	attr_reader :id, :version, :initialized, :destroyed


	def initialize(client, id, options)
		@client = client
		@id = id
		@destroyed = false
		@ondestroy_proc = nil
		@onstatuschange_proc = nil
		@initialized = !!options[:object]
		self.merge!(options[:object]) if options[:object].kind_of?(Hash)
	end


	def destroy(id)
		@destroyed = true
		@onstatuschange_proc.call(self) if @onstatuschange_proc
		@ondestroy_proc.call(self) if @ondestroy_proc
	end


	def matches(id)
		id =~ Regexp.new(Regexp.escape(@id).gsub('\*','.*?'))
	end


	def sanitized
		JSON.load(JSON.dump(self))
	end


	def ondestroy(&block)
		@ondestroy_proc = block
		self
	end


	def onstatuschange(&block)
		@onstatuschange_proc = block
		self
	end


	def unlink
		@client.unlink(@id)
	end

end



class SeapigSlaveObject < SeapigObject

	attr_reader :received_at, :valid


	def initialize(client, id, options)
		super(client, id, options)
		@version = (options[:version] or 0)
		@valid = false
		@received_at = nil
	end


	def onchange(&block)
		@onchange_proc = block
		self
	end

# ----- for SeapigClient

	def patch(message)
		@received_at = Time.new
		old_self = JSON.dump(self)
		if (not message['version-old']) or (message['version-old'] == 0) or message.has_key?('value')
			self.clear
		elsif not @version == message['version-old']
			raise "Seapig lost some updates, this should never happen: "+[self, @version, message].inspect
		end
		if message['value']
			self.merge!(message['value'])
		else
			Hana::Patch.new(message['patch']).apply(self)
		end
		@version = message['version-new']
		@valid = true
		@initialized = true
		@onstatuschange_proc.call(self) if @onstatuschange_proc
		@onchange_proc.call(self) if @onchange_proc and old_self != JSON.dump(self)
	end


	def validate
		@valid = @initialized
		@onstatuschange_proc.call(self) if @onstatuschange_proc
	end


	def invalidate
		@valid = false
		@onstatuschange_proc.call(self) if @onstatuschange_proc
	end


end


class SeapigMasterObject < SeapigObject

	attr_accessor :stall


	def initialize(client, id, options)
		super(client, id, options)
		@version = (options[:version] or [(Time.new.to_f*1000).floor, 0])
		@shadow = self.sanitized
		@stall = false
	end


	def onproduce(&block)
		@onproduce_proc = block
		self
	end


	def set(options={})
		@version = options[:version] if options[:version]
		if options[:object]
			@stall = false
			self.clear
			self.merge!(options[:object])
		elsif options[:object] == false or options[:stall]
			@stall = true
		end
		@shadow = sanitized
		@initialized = true
		upload(0, {}, @version, @stall ? false : @shadow)
	end


	def bump(options={})
		version_old = @version
		data_old = @shadow
		@version = (options[:version] or [version_old[0], version_old[1]+1])
		@shadow = sanitized
		@initialized = true
		upload(version_old, data_old, @version, @stall ? false : @shadow)
	end

# ----- for SeapigClient

 	def produce(id, version_inferred)
		if @onproduce_proc
			@onproduce_proc.call(self, version_inferred)
		else
			raise "Master object #{id} has to either be initialized at all times or have an onproduce callback" if not @initialized
			upload(0, {}, @version, @shadow)
		end
	end

private

	def upload(version_old, data_old, version_new, data_new)
		if @client.connected
			if version_old == 0 or data_new == false
				@client.socket.send JSON.dump(id: @id, action: 'object-patch', :"version-new" => version_new, value: data_new)
			else
				diff = JsonDiff.generate(data_old, data_new)
				if JSON.dump(diff).size < JSON.dump(data_new).size #can we afford this?
					@client.socket.send JSON.dump(id: @id, action: 'object-patch', :'version-old' => version_old, :'version-new' => version_new, patch: diff)
				else
					@client.socket.send JSON.dump(id: @id, action: 'object-patch', :'version-new' => version_new, value: data_new)
				end
			end
		end
		self
	end

end




class SeapigWildcardSlaveObject < SeapigSlaveObject


	def patch(message)
		self[message['id']] ||= SeapigSlaveObject.new(@client, message['id'],{}).onchange(&@onchange_proc)
		self[message['id']].patch(message)
	end


	def destroy(id)
		return if not destroyed = self.delete(id)
		destroyed.destroy(id)
	end

end



class SeapigWildcardMasterObject < SeapigMasterObject

	def initialize(client, id, options)
		super(client, id, options)
		@children = ObjectSpace::WeakMap.new
		@options = options
	end


	def [](id)
		key = @children.keys.find { |key| key == id }
		@children[(key or id)] ||= SeapigMasterObject.new(@client, id, @options)
	end


	def produce(id, version_inferred)
		child = self[id]
		if @onproduce_proc
			@onproduce_proc.call(child, version_inferred)
		else
			child.send
		end
	end


	def destroy(message)
		key = @children.keys.find { |key| key == id }
		return if not (key and destroyed = @children[key])
		destroyed.destroy(id)
	end

end
