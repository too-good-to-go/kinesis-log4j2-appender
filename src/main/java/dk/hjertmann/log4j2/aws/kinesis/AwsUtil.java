/*
 * Copyright (C) 2018 https://github.com/Minamoto54
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Modifications copyright (C) 2018 Robert Hjertmann Christiansen
 * 
 */
package dk.hjertmann.log4j2.aws.kinesis;

import com.amazonaws.ClientConfiguration;

class AwsUtil {

  public static ClientConfiguration setProxySettingsFromSystemProperties(
      final ClientConfiguration clientConfiguration) {

    final String proxyHost = System.getProperty("http.proxyHost");
    if (proxyHost != null) {
      clientConfiguration.setProxyHost(proxyHost);
    }

    final String proxyPort = System.getProperty("http.proxyPort");
    if (proxyPort != null) {
      clientConfiguration.setProxyPort(Integer.parseInt(proxyPort));
    }

    final String proxyUser = System.getProperty("http.proxyUser");
    if (proxyUser != null) {
      clientConfiguration.setProxyUsername(proxyUser);
    }

    final String proxyPassword = System.getProperty("http.proxyPassword");
    if (proxyPassword != null) {
      clientConfiguration.setProxyPassword(proxyPassword);
    }

    final String proxyDomain = System.getProperty("http.auth.ntlm.domain");
    if (proxyDomain != null) {
      clientConfiguration.setProxyDomain(proxyDomain);
    }

    final String workstation = System.getenv("COMPUTERNAME");
    if (proxyDomain != null && workstation != null) {
      clientConfiguration.setProxyWorkstation(workstation);
    }

    return clientConfiguration;
  }

}
