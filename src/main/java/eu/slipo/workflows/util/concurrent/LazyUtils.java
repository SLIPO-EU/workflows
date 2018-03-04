package eu.slipo.workflows.util.concurrent;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.lang3.tuple.Pair;


/**
 * Provide static utility methods for lazy-initialized objects  
 */
public class LazyUtils
{
    private LazyUtils() {}
    
    public static <V> Iterable<V> lazyIterable(Iterator<V> iterator)
    {
        Lazy<List<V>> list = 
            new Lazy<List<V>>(() -> IteratorUtils.toList(iterator));
        
        return () -> list.get().iterator();
    }
    
    public static <K,V> Lazy<Map<K,V>> lazyMap(Iterator<Pair<K,V>> pairIterator) 
    {
        return new Lazy<Map<K,V>>(() -> {
            Map<K,V> map = new HashMap<>();
            for (Pair<K,V> p: IteratorUtils.asIterable(pairIterator))
                map.put(p.getKey(), p.getValue());
            return map;
        });
    }
}