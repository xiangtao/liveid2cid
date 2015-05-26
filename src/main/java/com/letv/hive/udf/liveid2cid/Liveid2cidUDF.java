package com.letv.hive.udf.liveid2cid;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

/**
 * @author taox
 */
public class Liveid2cidUDF extends GenericUDF {
	private transient ObjectInspectorConverters.Converter[] converters;

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments)
			throws UDFArgumentException {
		if (arguments.length != 1) {
			throw new UDFArgumentLengthException(
					"The function INSTR accepts exactly 1 arguments.");
		}

		for (int i = 0; i < arguments.length; i++) {
			Category category = arguments[i].getCategory();
			if (category != Category.PRIMITIVE) {
				throw new UDFArgumentTypeException(i, "The "
						+ GenericUDFUtils.getOrdinal(i + 1)
						+ " argument of function INSTR is expected to a "
						+ Category.PRIMITIVE.toString().toLowerCase()
						+ " type, but " + category.toString().toLowerCase()
						+ " is found");
			}
		}
		converters = new ObjectInspectorConverters.Converter[arguments.length];
		for (int i = 0; i < arguments.length; i++) {
			converters[i] = ObjectInspectorConverters
					.getConverter(
							arguments[i],
							PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		}

		return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		if (arguments[0].get() == null) {
			return null;
		}
		Text text = (Text) converters[0].convert(arguments[0].get());
		String liveid = text.toString();
		String code = StringUtils.substring(liveid, 0, 2);
		Text cid = null;
		if ("10".equals(code)) {
			cid = new Text("4");
		} else if ("20".equals(code)) {
			cid = new Text("3");
		} else if ("30".equals(code)) {
			cid = new Text("9");
		}
		return cid;
	}

	@Override
	public String getDisplayString(String[] children) {
		assert (children.length == 1);
		return "liveid2cidUDF(" + children[0] + ")";
	}

}
