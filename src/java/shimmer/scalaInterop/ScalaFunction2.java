package shimmer.scalaInterop;

import clojure.lang.IFn;
import sparkling.serialization.Utils;
import scala.Function2;
import scala.runtime.AbstractFunction2;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class ScalaFunction2 extends AbstractFunction2 implements Function2, Serializable {

  private IFn f;

  public ScalaFunction2() {}

  public ScalaFunction2(IFn func) {
    f = func;
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    Utils.writeIFn(out, f);
  }
  
  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    f = Utils.readIFn(in);
  }

    @Override
    public Object apply(Object a, Object b) {
        return f.invoke(a, b);
    }
}
