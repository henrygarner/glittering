package glittering.scalaInterop;

import clojure.lang.IFn;
import sparkling.serialization.Utils;
import scala.Function3;
import scala.runtime.AbstractFunction3;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class ScalaFunction3 extends AbstractFunction3 implements Function3, Serializable {

  private IFn f;

  public ScalaFunction3() {}

  public ScalaFunction3(IFn func) {
    f = func;
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    Utils.writeIFn(out, f);
  }
  
  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    f = Utils.readIFn(in);
  }

    @Override
    public Object apply(Object a, Object b, Object c) {
        return f.invoke(a, b, c);
    }
}
