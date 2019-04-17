/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package common


// https://github.com/apache/flink/blob/master/flink-core/src/main/java/org/apache/flink/util/OperatingSystem.java
private object OperatingSystem {
  sealed trait Os
  case object Linux extends Os
  case object Windows extends Os
  case object Mac extends Os
  case object Solaris extends Os
  case object FreeBSD extends Os
  case object Unknown extends Os

  def get(): Os = {
    val osName = System.getProperty(OS_KEY)

    if (osName.startsWith(LINUX_OS_PREFIX)) {
      Linux
    } else if (osName.startsWith(WINDOWS_OS_PREFIX)) {
      Windows
    } else if (osName.startsWith(MAC_OS_PREFIX)) {
      Mac
    } else if (osName.startsWith(FREEBSD_OS_PREFIX))  {
      FreeBSD
    } else {
      val osNameLowerCase = osName.toLowerCase
      if (osNameLowerCase.contains(SOLARIS_OS_INFIX_1) || osNameLowerCase.contains(SOLARIS_OS_INFIX_2))
        Solaris
      else
        Unknown
    }
  }

  /**
    * The key to extract the operating system name from the system properties.
    */
  private val OS_KEY = "os.name"

  /**
    * The expected prefix for Linux operating systems.
    */
  private val LINUX_OS_PREFIX = "Linux"

  /**
    * The expected prefix for Windows operating systems.
    */
  private val WINDOWS_OS_PREFIX = "Windows"

  /**
    * The expected prefix for Mac OS operating systems.
    */
  private val MAC_OS_PREFIX = "Mac"

  /**
    * The expected prefix for FreeBSD.
    */
  private val FREEBSD_OS_PREFIX = "FreeBSD"

  /**
    * One expected infix for Solaris.
    */
  private val SOLARIS_OS_INFIX_1 = "sunos"

  private val SOLARIS_OS_INFIX_2 = "solaris"
}
