import time
import network
import ubinascii
import machine
import ujson
from umqtt_robust import MQTTClient
from utility import say
import gc

class MQTT:
    def __init__(self):
        self.client = None
        self.server = ''
        self.username = ''
        self.password = ''
        self.wifi_ssid = ''
        self.wifi_password = ''
        self.callbacks = {}
        self.last_sent = 0
        self.virtual_pins = {}
        self.virtual_pin_values = {}
        self.subscribed_pins = set()
        self._topic_cache = {}

    def __on_receive_message(self, topic: bytes, msg: bytes) -> None:
        topic_str = topic.decode('utf-8')
        payload   = msg.decode('utf-8')
        # print(f"[MQTT] Received message → topic='{topic_str}', payload='{payload}'")
        try:
            data = ujson.loads(payload)
        except Exception as e:
            print(f"[MQTT]  JSON decode error: {e}")
            return
        if callable(self.callbacks.get(topic_str)):
            self.callbacks[topic_str](payload)

    def connect_wifi(self, ssid: str, password: str, wait_for_connected: bool = True) -> None:
        self.wifi_ssid = ssid
        self.wifi_password = password
        say('Connecting to WiFi...')
        self.station = network.WLAN(network.STA_IF)
        if self.station.active():
            self.station.active(False)
            time.sleep_ms(500)

        for i in range(5):
            try:
                self.station.active(True)
                self.station.connect(ssid, password)
                break
            except OSError:
                self.station.active(False)
                time.sleep_ms(500)
                if i == 4:
                    say('Failed to connect to WiFi')
                    raise

        if wait_for_connected:
            count = 0
            while not self.station.isconnected():
                count += 1
                if count > 300:  # ~30 seconds
                    say('Failed to connect to WiFi')
                    raise
                time.sleep_ms(100)

            ip = self.station.ifconfig()[0]
            say(f'WiFi connected. IP: {ip}')

    def wifi_connected(self) -> bool:
        return self.station.isconnected()
    
    # Check and reconnect WiFi and MQTT if needed
    def _check_and_reconnect(self):
        """
        Private method to check connections and reconnect automatically.
        This is called internally by other methods.
        """
        gc.collect()
        try:
            # only proceed if client exists
            if not self.client:
                return

            # 1. Check WiFi connection
            if not self.station.isconnected():
                say("WiFi disconnected. Attempting to reconnect...")
                # Disconnect old MQTT client
                try:
                    self.client.disconnect()
                except:
                    pass

                # Reconnect WiFi
                self.connect_wifi(self.wifi_ssid, self.wifi_password)

                # Reconnect Broker and re-subscribe all topics
                self.connect_broker(self.server, self.port, self.username, self.password)
                self.resubscribe()
                say("Reconnection successful.")

            # 2. If WiFi is still OK, umqtt_robust will handle MQTT reconnect automatically
            # We just need to call any command to trigger it if needed
            # self.client.ping() # Safe ping() command to check

        except Exception as e:
            say(f"Auto-reconnect failed: {e}")
            time.sleep(5) # Wait 5 seconds before trying again on the next call

    def connect_broker(self,
                    server: str = 'mqtt1.eoh.io',
                    port:   int = 1883,
                    username: str = '',
                    password: str = '') -> None:
        self.server = server
        self.port = port
        self.password = password

        self.username = username
        
        client_id = ubinascii.hexlify(machine.unique_id()).decode() \
                    + str(time.ticks_ms())

        # 1) Create client and connect
        # self.client = MQTTClient(client_id, server, port, username, password)
        
            # --- BẮT ĐẦU LOGIC LAST WILL AND TESTAMENT (LWT) ---

        # 1. Chuẩn bị nội dung cho "di chúc" (Last Will)
        online_topic = f"eoh/chip/{username}/is_online"
        online_payload_offline = f'{{"ol":0}}'
        
        # 2. Khởi tạo client MQTT
        self.client = MQTTClient(client_id, server, port, user=username, password=password, keepalive=60)

        # 3. Đăng ký "di chúc" với client bằng phương thức set_last_will()
        #    Việc này phải được thực hiện SAU KHI tạo client và TRƯỚC KHI kết nối.
        self.client.set_last_will(
            online_topic,
            online_payload_offline,
            retain=True,
            qos=1
        )
        
        try:
            self.client.disconnect()
        except:
            pass
        self.client.connect()
        self.client.set_callback(self.__on_receive_message)
        say('Connected to MQTT broker')
        
        # online_topic = f"eoh/chip/{username}/is_online"
        # online_payload = f'{{"ol":0}}'
        # self.client.publish(online_topic, online_payload, retain=True, qos=1)
        # time.sleep_ms(500)
        
        self.subscribed_pins.clear()

        
        # 2) Subscribe topic down with handler
        down_topic = f"eoh/chip/{username}/down"
        self.callbacks[down_topic] = self._handle_config_down  # Register callback
        self.client.subscribe(down_topic)  # Subscribe
        # say(f"Subscribed to {down_topic}")

        # 3) Wait to ensure subscription is complete
        time.sleep_ms(500)

        # 4) Send online with ask_configuration
        online_topic = f"eoh/chip/{username}/is_online"
        online_payload = f'{{"ol":1,"ask_configuration":1}}'
        self.client.publish(online_topic, online_payload, retain=True, qos=1)
        # say(f'Announced online with config request')

        # 5) Wait for and process config message
        # say('Waiting for configuration...')
        timeout = 0
        while len(self.virtual_pins) == 0 and timeout < 50:  # Wait max 5 seconds
            self.client.check_msg()  # Check for incoming messages
            time.sleep_ms(100)
            timeout += 1
        
        # if len(self.virtual_pins) > 0:
        #     say(f'Configuration received: {len(self.virtual_pins)} pins configured')
        # else:
        #     say('Warning: No configuration received')


    def subscribe_config_down(self, token: str, callback=None) -> None:
        """
        Subscribe topic eoh/chip/{token}/down.
        If no callback provided, use internal handler to populate virtual_pins.
        """
        topic = f"eoh/chip/{token}/down"
        cb = callback or self._handle_config_down
        self.on_receive_message(topic, cb)

    def _handle_config_down(self, msg: str) -> None:
        """
        Default handler for config/down messages.
        Parses JSON and fills self.virtual_pins.
        """
        data = ujson.loads(msg)
        devices = data.get('configuration', {}) \
                      .get('arduino_pin', {}) \
                      .get('devices', [])
                      
        
        self.virtual_pins.clear()      
        self._topic_cache.clear()        
                      
        for d in devices:
            for v in d.get('virtual_pins', []):
                pin    = int(v['pin_number'])
                cfg_id = int(v['config_id'])
                self.virtual_pins[pin] = cfg_id
        # print("Config received, pin→config_id:", self.virtual_pins)

    def on_receive_message(self, topic: str, callback) -> None:
        """
        Subscribe an arbitrary topic and register a callback.
        """
        full_topic = topic
        self.callbacks[full_topic] = callback
        self.client.subscribe(full_topic)
        # say(f"Subscribed to {full_topic}")

    def resubscribe(self) -> None:
        """
        Re-subscribe to all topics after reconnect.
        """
        for t in self.callbacks.keys():
            self.client.subscribe(t)

    def virtual_write(self, pin: int, value, username: str = '', 
                      qos: int = 0, debug: bool = False) -> bool:
        """
        Publish a value to a virtual pin (optimized for Blockly).
        
        Args:
            pin: Virtual pin number (0-499)
            value: Value to send (int, float, string, bool)
            username: MQTT token (default: use self.username)
            qos: Quality of Service (0=fast, 1=reliable, default=0)
            debug: Enable debug logging (default=False)
        
        Returns:
            bool: True if successful, False if failed
        """
        self._check_and_reconnect()
        
        if self.client:
            self.client.check_msg()
        
        if debug:
            print(f"[V{pin}] Sending: {value}")
        
        if pin not in self.virtual_pins:
            if debug:
                print(f"[Error] V{pin} not configured")
            return False
        
        cfg_id = self.virtual_pins[pin]
        token = username or self.username
        # topic = f"eoh/chip/{token}/config/{cfg_id}/value"
        
        if pin not in self._topic_cache:
            # Nếu topic cho pin này chưa có trong cache, tạo mới và lưu lại
            cfg_id = self.virtual_pins[pin]
            token = username or self.username
            self._topic_cache[pin] = f"eoh/chip/{token}/config/{cfg_id}/value"
        
        # Luôn lấy topic từ cache để tái sử dụng
        topic = self._topic_cache[pin]
        
        # Handle different value types
        if isinstance(value, bool):
            # True → 1, False → 0
            payload = f'{{"v": {1 if value else 0}}}'
        elif isinstance(value, str):
            # String cần quotes
            payload = f'{{"v": "{value}"}}'
        else:
            # Number (int, float)
            payload = f'{{"v": {value}}}'
        
        # Publish với throttle nhẹ
        try:
            # Throttle 10ms (thay vì 100ms)
            now = time.ticks_ms()
            elapsed = time.ticks_diff(now, self.last_sent)
            if elapsed < 10:
                time.sleep_ms(10 - elapsed)
            
            # Publish (qos=0 mặc định cho speed)
            self.client.publish(topic, payload.encode('utf-8'), retain=True, qos=0)
            self.last_sent = time.ticks_ms()
            
            if debug:
                print(f"[V{pin}] Sent successfully")
            return True
            
        except Exception as e:
            if debug:
                print(f"[Error] Publish failed: {e}")
            return False
        
    def subscribe_virtual_pin(self, pin: int, token: str, callback=None) -> None:
        """
        Subscribe to topic eoh/chip/{token}/virtual_pin/{pin_number}
        to receive data from virtual pin.
        """
        topic = f"eoh/chip/{token}/virtual_pin/{pin}"
        
        if callback is None:
            # Tạo callback wrapper để lưu pin number
            def pin_callback(msg):
                self._handle_virtual_pin_data(msg, pin)
            cb = pin_callback
        else:
            cb = callback
            
        self.on_receive_message(topic, cb)
        # say(f"Subscribed to virtual pin V{pin}")
        self.subscribed_pins.add(pin)


    def _handle_virtual_pin_data(self, msg: str, pin: int = None) -> None:
        """
        Default handler for virtual pin data messages.
        Parses JSON and stores the received value.
        Expected format: {"value": 5, "trigger_id": 2509389}
        """
        try:
            data = ujson.loads(msg)
            value = data.get('value', 'Unknown')
            trigger_id = data.get('trigger_id', None)
            # print(f"[Virtual Pin V{pin}] Received value: {value}, trigger_id: {trigger_id}")
            
            # Lưu giá trị vào dict nếu có pin number
            if pin is not None:
                self.virtual_pin_values[pin] = {
                    "value": value, 
                    "trigger_id": trigger_id,
                    "timestamp": time.ticks_ms()
                }
        except Exception as e:
            print(f"[Virtual Pin] JSON decode error: {e}")
            print(f"[Virtual Pin] Raw message: {msg}")

    def get_virtual_pin_value(self, pin: int) -> dict:
        """
        Get the latest value received from a virtual pin.
        Returns dict with 'value', 'trigger_id', 'timestamp' or None if no data.
        """
        return self.virtual_pin_values.get(pin, None)
    
    def get_virtual_pin_simple_value(self, pin: int) -> any:
        """
        Get only the value (not trigger_id/timestamp) from a virtual pin.
        Returns the value or None if no data.
        """
        data = self.virtual_pin_values.get(pin, None)
        return data.get('value', None) if data else None    
    
    def subscribe_and_get(self, pin: int, token: str) -> any:
        self._check_and_reconnect()
        
        newly_subscribed = False
        if pin not in self.subscribed_pins:
            self.subscribe_virtual_pin(pin, token)
            newly_subscribed = True

        if self.client:
           
            if newly_subscribed:
                
                timeout = 0
                
                while self.get_virtual_pin_simple_value(pin) is None and timeout < 30:
                    self.client.check_msg()
                    time.sleep_ms(100)
                    timeout += 1
            else:
                
                self.client.check_msg()

        return self.get_virtual_pin_simple_value(pin)

mqtt = MQTT()





