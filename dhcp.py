#!/usr/bin/python3
from scapy.all import *
from threading import Thread
from time import sleep
from sys import argv
from os import system

# Настройка интерфейса из аргументов командной строки и отключение вывода scapy
conf.iface = argv[1]
conf.verb = 0

# Глобальная переменная для хранения обнаруженных DHCP серверов
dhcp_servers = set()

def parse(p):
    """Функция для обработки DHCP пакетов и извлечения информации о серверах"""
    global dhcp_servers
    if DHCP in p:
        # Обработка DHCPv4
        for option in p[DHCP].options:
            if 'message-type' in option and 2 in option:  # DHCP Offer
                dhcp_servers.add(p[IP].src)
    elif DHCP6_Advertise in p:
        # Обработка DHCPv6
        dhcp_servers.add(p[IPv6].src)
        try:
            # Попытка извлечь DNS домены из DHCPv6
            domains = ','.join(p[DHCP6OptDNSDomains].dnsdomains)
            print(domains)
        except:
            pass

# Список для отслеживания уже оповещенных серверов
alerts = []

def alert(new_dhcp_servers):
    """Функция для оповещения о новых неавторизованных DHCP серверах"""
    if not new_dhcp_servers - set(alerts):
        return  # Пропускаем уже оповещенные серверы
    
    dhcp_servers = ", ".join(map(str, new_dhcp_servers))
    print("[!] DHCP roque: " + dhcp_servers)
    
    # Генерация GUI-оповещения
    system("zenity --warning --title='DHCP roque server' --text='DHCP roque: %s' &" % dhcp_servers)
    # Альтернативное голосовое оповещение (закомментировано)
    # system("echo 'DHCP roque server detected' | festival --tts --language english &")
    
    alerts.extend(new_dhcp_servers)

def dhcp_discover():
    """Функция для отправки DHCP discover пакетов (IPv4 и IPv6)"""
    # DHCPv4 discover
    dhcp_discover = Ether(dst='ff:ff:ff:ff:ff:ff', src=Ether().src, type=0x0800) / \
                    IP(src='0..0.0', dst='255.255.255.255') / \
                    UDP(dport=67, sport=68) / \
                    BOOTP(op=1, chaddr=Ether().src, xid=RandInt()) / \
                    DHCP(options=[('message-type','discover'), 
                                ('hostname','localhost'), 
                                ('param_req_list',[1,3,6]), 
                                ('end')])
    sendp(dhcp_discover)
    
    # DHCPv6 discover
    dhcp_discover6 = Ether(dst="33:33:00:01:00:02", src=Ether().src) / \
                    IPv6(dst="ff02::1:2") / \
                    UDP(sport=546, dport=547) / \
                    DHCP6_Solicit(trid=RandInt()) / \
                    DHCP6OptClientId(duid=DUID_LLT(lladdr=Ether().src, timeval=int(time.time()))) / \
                    DHCP6OptIA_NA(iaid=0xf) / \
                    DHCP6OptRapidCommit() / \
                    DHCP6OptElapsedTime() / \
                    DHCP6OptOptReq(reqopts=[23,24])
    sendp(dhcp_discover6)

# Множество для хранения авторизованных DHCP серверов
dhcp_servers_legal = set()

# Основной цикл работы
while True:
    dhcp_servers = set()
    
    # Запуск сниффера в отдельном потоке с таймаутом 5 секунд
    thr = Thread(target=lambda: sniff(timeout=5, prn=parse))
    thr.start()
    
    # Отправка discover пакетов
    dhcp_discover()
    
    # Ожидание завершения работы сниффера
    thr.join()
    
    # Если список авторизованных серверов пуст, инициализируем его
    if not dhcp_servers_legal:
        dhcp_servers_legal = dhcp_servers.copy() or set([""])
        print("[*] DHCP legal: " + ", ".join(map(str, dhcp_servers_legal)))
    
    # Проверка на наличие неавторизованных серверов
    if dhcp_servers - dhcp_servers_legal:
        alert(dhcp_servers - dhcp_servers_legal)
    
    # Пауза перед следующей итерацией
    sleep(10)