package com.lg;

import java.net.InetAddress;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class TestCon {
	private static String host="192.168.40.100";
	private static int port=9300;
	
	public static void main(String[] args)throws Exception {
		TransportClient transportClient=new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host),port));
		System.out.println(transportClient);
		transportClient.close();
	}
}
