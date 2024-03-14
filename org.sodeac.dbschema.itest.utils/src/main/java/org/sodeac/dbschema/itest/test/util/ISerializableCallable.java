package org.sodeac.dbschema.itest.test.util;

import java.io.Serializable;
import java.util.concurrent.Callable;

public interface ISerializableCallable extends Callable<TestConnection>, Serializable {}