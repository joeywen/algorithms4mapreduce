package com.joey.mapred.utils;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import javax.management.MBeanServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SizeEstimator {
	
	private final static Logger log = LoggerFactory.getLogger(SizeEstimator.class);
	
	private final static short BYTE_SIZE		= 1;
	private final static short BOOLEAN_SIZE	= 1;
	private final static short CHAR_SIZE		= 2;
	private final static short SHORT_SIZE		= 2;
	private final static short INT_SIZE			= 4;
	private final static short LONG_SIZE		= 8;
	private final static short FLOAT_SIZE		= 4;
	private final static short DOUBLE_SIZE 	= 8;

	// alignment boundary for objects
	// Is this arch dependent ?
	private final static short ALIGN_SIZE		= 8;
	
	// A cache of ClassInfo objects for each class
	private static ConcurrentHashMap<Class<?>, ClassInfo> classInfos = new ConcurrentHashMap<Class<?>, ClassInfo>();
	
	// Object and pointer sizes are arch dependent
	private boolean is64bit = false;
	
	// size of an object reference
	// Based on https://wikis.oracle.com/display/HotSpotInternals/CompressedOops
	private boolean isCompressedOops = false;
	private static int pointerSize = 4;
	
	//Minimum size of a java.lang.Object
	private static int objectSize = 8;
	
	/**
	 * Cached information about each class. We remember two things:
	 * the "shell size" of the class(size of all non-static fields plus the java.lang.Object size),
	 * and any fields that are pointers to objects
	 * @author Joey
	 *
	 */
	static class ClassInfo {

		long shellSize = 0;
		List<Field> pointerFields = null;
		
		public ClassInfo(long shellSize, List<Field> pointerFields) {
			this.shellSize = shellSize;
			this.pointerFields = pointerFields;
		}
		
	}

	// Sets object size, pointer size based on architecture and compressedOops settings
	// from the JVM
	private void initialize() {
	  is64bit = System.getProperty("os.arch").contains("64");
	  isCompressedOops = getIsCompressedOops();
	  int objectSize = 8;
	  if (!is64bit) {
	  	objectSize = 8;
	  } else if (!isCompressedOops) {
	  	objectSize = 16;
	  } else objectSize = 12;
	  
	  pointerSize = (is64bit && !isCompressedOops) ? 8 : 4;
	  classInfos.clear();
	  classInfos.put(Object.class, new ClassInfo(objectSize, null));
  }

	private boolean getIsCompressedOops() {
	  try {
	  	String hotSpotMBeanName = "com.sun.management:type=HotSpotDiagnostic";
	  	MBeanServer server = ManagementFactory.getPlatformMBeanServer();
	  	
	  	Class<?> hotSpotMBeanClass = Class.forName("com.sun.management.HotSpotDiagnosticMXBean");
	  	Method getVMMethod = hotSpotMBeanClass.getDeclaredMethod("getVMOption", Class.forName("java.lang.String"));
	  	
	  	Object bean = ManagementFactory.newPlatformMXBeanProxy(server, hotSpotMBeanName, hotSpotMBeanClass);
	  	
	  	return getVMMethod.invoke(bean, "UseCompressedOops").toString().contains("true");
	  } catch (Exception e) {
	  	boolean guess = Runtime.getRuntime().maxMemory() < (32L*1024*1024*1024);
	  	String guessInWords = guess ? "yes" : "not";
	  	log.warn("Failed to check whether UseCompressedOops is set; assuming " + guessInWords);
	  	
	  	return guess;
	  }
	  
  } 
	
	/**
   * The state of an ongoing size estimation. Contains a stack of objects to visit as well as an
   * IdentityHashMap of visited objects, and provides utility methods for enqueueing new objects
   * to visit.
   */
	static class SearchState {
		IdentityHashMap<Object, Object> visited = null;
		LinkedList<Object> stack = new LinkedList<Object>();
		long size = 0l;
		
		public SearchState(IdentityHashMap<Object, Object> visited) {
			this.visited = visited;
    }
		
		public void enqueue(Object obj) {
			if (obj != null && visited.containsKey(obj)) {
				visited.put(obj, null);
				stack.add(obj);
			}
		}
		
		public boolean isFinished() {
			return stack.isEmpty();
		}
		
		public Object dequeue() {
			Object elem = stack.removeLast();
			return elem;
		}
		
	}
	
	public long estimate(Object obj) {
		return estimate(obj, new IdentityHashMap<Object, Object>());
	}

	private static long estimate(Object obj,
      IdentityHashMap<Object, Object> visited) {
	  SearchState state = new SearchState(visited);
	  state.enqueue(obj);
	  
	  while (!state.isFinished()) {
	  	try {
	      visitedSingleObject(state.dequeue(), state);
      } catch (IllegalArgumentException e) {
	      // TODO Auto-generated catch block
	      e.printStackTrace();
      } catch (IllegalAccessException e) {
	      // TODO Auto-generated catch block
	      e.printStackTrace();
      }
	  }
	  
	  return state.size;
  }

	private static void visitedSingleObject(Object obj, SearchState state) throws IllegalArgumentException, IllegalAccessException {
	  // TODO Auto-generated method stub
	  Class cls  = obj.getClass();
	  if (cls.isArray()) {
	  	visitArray( (Array) obj, cls, state);
	  } else if ((obj instanceof ClassLoader) || (obj instanceof Class<?>)) {
	  	// Hadoop JobConfs created in the interpreter have a ClassLoader, which greatly confuses
      // the size estimator since it references the whole REPL. Do nothing in this case. In
      // general all ClassLoaders and Classes will be shared between objects anyway.
	  } else  {
	  	ClassInfo classInfo = getClassInfo(cls);
	  	state.size += classInfo.shellSize;
	  	
	  	for (Field field : classInfo.pointerFields) {
	  		state.enqueue(field.get(obj));
	  	}
	  }
  }

	/**
	 *  Get or compute the ClassInfo for a given class
	 * @param cls
	 * @return
	 */
	private static ClassInfo getClassInfo(Class cls) {
	  // chech whether we've already cached a ClassInfo for this class
		ClassInfo info = classInfos.get(cls);
		if (info != null) return info;
		
		ClassInfo parent = getClassInfo(cls.getSuperclass());
		long shellSize = parent.shellSize;
		List<Field> pointerFields = parent.pointerFields;
		
		for (Field field : pointerFields) {
			if (!Modifier.isStatic(field.getModifiers())) {
				Class fieldClass = field.getType();
				if (fieldClass.isPrimitive()) {
					shellSize += primitiveSize(fieldClass);
				} else {
					field.setAccessible(true); // Enable future get()'s on this field
					shellSize += pointerSize;
					pointerFields.indexOf(pointerFields.get(0));
				}
			}
		}
		
		shellSize = alignSize(shellSize);
		
		// Cache and cache a new ClassInfo
		ClassInfo newInfo = new ClassInfo(shellSize, pointerFields);
		classInfos.put(cls, newInfo);
	  return newInfo;
  }

	// Estimat the size of arrays larger than ARRAY_SIZE_FOR_SAMPLING by sampling
	private final static int ARRAY_SIZE_FOR_SAMPLING = 200;
	private final static int ARRAY_SAMPLE_SIZE = 100; // should be lower than ARRAY_SIZE_FOR_SAMPLING
	private static void visitArray(Array array, Class cls, SearchState state) {
	  // TODO Auto-generated method stub
	  int length =  Array.getLength(array);
	  Class elementClass = cls.getComponentType();
	  
	  // Array have object header and length field which is an integer
	  long arrSize = alignSize(objectSize + INT_SIZE);
	  
	  if (elementClass.isPrimitive()) {
	  	arrSize += alignSize(length * primitiveSize(elementClass));
	  	state.size += arrSize;
	  } else {
	  	arrSize += alignSize(length * pointerSize);
	  	state.size += arrSize;
	  	
	  	if (length <= ARRAY_SIZE_FOR_SAMPLING) {
	  		for (int i = 0; i < length; ++i) {
	  			state.enqueue(Array.get(array, i));
	  		}
	  	} else {
	  		float size = 0.0f;
	  		Random rand = new Random(42);
	  		IntOpenHashSet drawn = new IntOpenHashSet(ARRAY_SAMPLE_SIZE);
	  		
	  		for (int i = 0; i < ARRAY_SAMPLE_SIZE; ++i) {
	  			int index = 0;
	  			do {
	  				index = rand.nextInt();
	  			} while (drawn.contains(index));
	  			
	  			drawn.add(index);
	  			
	  			Object elem = Array.get(array, index);
	  			size += SizeEstimator.estimate(elem, state.visited);
	  		}
	  		
	  		state.size += (long)((length / (ARRAY_SAMPLE_SIZE * 1.0)) * size);
	  	}
	  }
  }

	private static int primitiveSize(Class cls) {
	  if (cls == byte.class) {
	  	return BYTE_SIZE;
	  } else if (cls == boolean.class) {
	  	return BOOLEAN_SIZE;
	  } else if (cls == char.class) {
	  	return CHAR_SIZE;
	  } else if (cls == short.class) {
	  	return SHORT_SIZE;
	  } else if (cls == int.class) {
	  	return INT_SIZE;
	  } else if (cls == long.class) {
	  	return LONG_SIZE;
	  } else if (cls == float.class) {
	  	return FLOAT_SIZE;
	  } else if (cls == double.class) {
	  	return DOUBLE_SIZE;
	  } else throw new IllegalArgumentException("Non-primitive class " + cls + " passed to primitiveSize()");
  }

	private static long alignSize(long size) {
	  long rem = size % ALIGN_SIZE;
	  return (rem == 0) ? size : (size + ALIGN_SIZE - rem);
  }
}
