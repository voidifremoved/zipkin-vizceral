package com.rubberjam.vizceral;

import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.springframework.http.HttpMethod;
import org.springframework.http.client.ClientHttpRequest;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.util.FileCopyUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

@RestController
public class ZipkinTranslatorController
{
	
	private final HttpComponentsClientHttpRequestFactory requestFactory = new HttpComponentsClientHttpRequestFactory();

    @RequestMapping(name="/data", method=RequestMethod.GET)
    public VizceralNode data(@RequestParam(value="until", required=false) Long until, 
    		@RequestParam(value="minutes", defaultValue="60") int minutes, 
    		@RequestParam(value="server") String server) throws Exception {
    	if (until == null)
    	{
    		until = System.currentTimeMillis();
    	}
    	
    	String path = String.format("http://%s/api/v1/dependencies?endTs=%s&lookback=%s", server, until, (minutes * 60 * 1000));
		ClientHttpRequest req = requestFactory.createRequest(URI.create(path), HttpMethod.GET);
		ObjectMapper mapper = new ObjectMapper();
		String body = FileCopyUtils.copyToString(new InputStreamReader(req.execute().getBody()));
		List<ZipkinNode> nodes = mapper.readValue(body, mapper.getTypeFactory().constructCollectionType(List.class, ZipkinNode.class));
    	
        return translate(nodes);
    }
    
	private VizceralNode translate(List<ZipkinNode> nodes)
	{
		Map<ZipkinNode, ZipkinNode> allZipkinNodes = new LinkedHashMap<>();
		
		Map<String, TrafficNode> allNodes = new TreeMap<>();
		
		Map<String, TrafficNode> rootNodes = new TreeMap<>();
		
		for (int i = 0; i < nodes.size(); i++)
		{
			ZipkinNode node = nodes.get(i);
			allZipkinNodes.put(node, node);
			
		}
		

		int totalRootCalls = 0;
		for (ZipkinNode node : allZipkinNodes.keySet())
		{

			TrafficNode parent = allNodes.get(node.getParent());
			if (parent == null)
			{
				parent = new TrafficNode(node.getParent());
				allNodes.put(parent.getName(), parent);
			}
			
			if (node.getChild().equals(node.getParent()))
			{
				rootNodes.put(parent.getName(), parent);

				totalRootCalls += node.getCallCount();
			}
			
			else
			{
				TrafficNode child = allNodes.get(node.getChild());

				if (child == null)
				{
					child = new TrafficNode(node.getChild());
					allNodes.put(child.getName(), child);
				}
				allNodes.put(node.getChild(), child);
				parent.addTraffic(child, node.getCallCount());
			}
		}
		
		
		List<VizceralNode> translated = new ArrayList<>();
		VizceralNode global = new VizceralNode();
		global.setName("edge");
		global.setRenderer("global");
		
		VizceralNode internet = new VizceralNode();
		internet.setName("INTERNET");
		internet.setRenderer("region");
		
		VizceralNode api = new VizceralNode();
		api.setName("api");
		api.setRenderer("region");
		
		global.getNodes().add(internet);
		global.getNodes().add(api);
		VizceralConnection internetConnection = new VizceralConnection();
		internetConnection.setSource(internet.getName());
		internetConnection.setTarget(api.getName());
		internetConnection.getMetrics().setNormal(totalRootCalls);
		global.getConnections().add(internetConnection);
		
		for (TrafficNode node : allNodes.values())
		{
			VizceralNode n = new VizceralNode();
			n.setName(node.getName());
			n.setRenderer("focusedChild");
			n.setUpdated(System.currentTimeMillis());
			n.setDisplayClass("normal");
			node.getTraffic().values().forEach((traffic) -> {
				VizceralConnection c = new VizceralConnection();
				c.setSource(n.getName());
				c.setTarget(traffic.getNode().getName());
				c.getMetrics().setNormal(traffic.getCallCount());
				c.setDisplayClass("normal");
				api.getConnections().add(c);
			});
			translated.add(n);
		}
		

		api.setNodes(translated);
		return api;
	}

	private static final class ZipkinNode {
		
		private String parent;
		
		private String child;
		
		private int callCount;

		public final String getParent()
		{
			return parent;
		}

		public final void setParent(String parent)
		{
			this.parent = parent;
		}

		public final String getChild()
		{
			return child;
		}

		public final void setChild(String child)
		{
			this.child = child;
		}

		public final int getCallCount()
		{
			return callCount;
		}

		public final void setCallCount(int callCount)
		{
			this.callCount = callCount;
		}

		@Override
		public String toString()
		{
			return "ZipkinNode [parent=" + parent + ", child=" + child + ", callCount=" + callCount + "]";
		}

		@Override
		public int hashCode()
		{
			final int prime = 31;
			int result = 1;
			result = prime * result + ((child == null) ? 0 : child.hashCode());
			result = prime * result + ((parent == null) ? 0 : parent.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj)
		{
			if (this == obj) return true;
			if (obj == null) return false;
			if (getClass() != obj.getClass()) return false;
			ZipkinNode other = (ZipkinNode) obj;
			if (child == null)
			{
				if (other.child != null) return false;
			}
			else if (!child.equals(other.child)) return false;
			if (parent == null)
			{
				if (other.parent != null) return false;
			}
			else if (!parent.equals(other.parent)) return false;
			return true;
		}
		
		
		
	}
	
	private static final class TrafficNode {
		
		private String name;
		
		private Map<String, Traffic> traffic = new TreeMap<>();

		
		
		public TrafficNode(String name)
		{
			this.name = name;
		}

		public final String getName()
		{
			return name;
		}

		public final void setName(String name)
		{
			this.name = name;
		}
		
		public void addTraffic(TrafficNode trafficTo, int callCount)
		{
			traffic.put(trafficTo.getName(), new Traffic(trafficTo, callCount));
		}

		public final Map<String, Traffic> getTraffic()
		{
			return traffic;
		}
		
		
		
	}
	
	private static final class Traffic {
		
		private TrafficNode node;
		
		private int callCount;

		
		
		public Traffic(TrafficNode node, int callCount)
		{
			this.node = node;
			this.callCount = callCount;
		}

		public final TrafficNode getNode()
		{
			return node;
		}

		public final void setNode(TrafficNode node)
		{
			this.node = node;
		}

		public final int getCallCount()
		{
			return callCount;
		}

		public final void setCallCount(int callCount)
		{
			this.callCount = callCount;
		}
		
		
		
	}
	
	
	
	private static final class VizceralNode
	{
		
		private String name;
		
		private String renderer;
		
		private int maxVolume;
		
		private long updated;
		
		
		@JsonProperty("class")
		private String displayClass;
		
		private List<VizceralNode> nodes = new ArrayList<>();
		
		private List<VizceralConnection> connections = new ArrayList<>();

		public final String getName()
		{
			return name;
		}

		public final void setName(String name)
		{
			this.name = name;
		}

		public final String getRenderer()
		{
			return renderer;
		}

		public final void setRenderer(String renderer)
		{
			this.renderer = renderer;
		}

		
		
		public final String getDisplayClass()
		{
			return displayClass;
		}

		public final void setDisplayClass(String displayClass)
		{
			this.displayClass = displayClass;
		}

		public final long getUpdated()
		{
			return updated;
		}

		public final void setUpdated(long updated)
		{
			this.updated = updated;
		}

		public final int getMaxVolume()
		{
			return maxVolume;
		}

		public final void setMaxVolume(int maxVolume)
		{
			this.maxVolume = maxVolume;
		}

		public final List<VizceralNode> getNodes()
		{
			return nodes;
		}

		public final void setNodes(List<VizceralNode> nodes)
		{
			this.nodes = nodes;
		}

		public final List<VizceralConnection> getConnections()
		{
			return connections;
		}

		public final void setConnections(List<VizceralConnection> connections)
		{
			this.connections = connections;
		}
		
		
		
		
	}
	
	private static final class VizceralConnection
	{
		private String source;
		
		private String target;
		
		@JsonProperty("class")
		private String displayClass;
		
		private VizceralMetrics metrics = new VizceralMetrics();

		public final String getSource()
		{
			return source;
		}

		public final void setSource(String source)
		{
			this.source = source;
		}

		public final String getTarget()
		{
			return target;
		}

		public final void setTarget(String target)
		{
			this.target = target;
		}

		public final VizceralMetrics getMetrics()
		{
			return metrics;
		}

		public final void setMetrics(VizceralMetrics metrics)
		{
			this.metrics = metrics;
		}

		public final String getDisplayClass()
		{
			return displayClass;
		}

		public final void setDisplayClass(String displayClass)
		{
			this.displayClass = displayClass;
		}
		
		
		
	}
	
	private static final class VizceralMetrics
	{
		
		private float danger;
		
		private float normal;

		public final float getDanger()
		{
			return danger;
		}

		public final void setDanger(float danger)
		{
			this.danger = danger;
		}

		public final float getNormal()
		{
			return normal;
		}

		public final void setNormal(float normal)
		{
			this.normal = normal;
		}
		
		
	}
}
