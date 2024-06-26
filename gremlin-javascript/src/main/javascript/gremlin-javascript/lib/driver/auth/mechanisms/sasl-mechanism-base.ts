/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

import { AuthenticatorOptions } from '../authenticator.js';

export type SaslMechanismBaseOptions = AuthenticatorOptions & { authzid?: string };

export default abstract class SaslMechanismBase {
  constructor(protected options: SaslMechanismBaseOptions) {
    this.setopts(options);
  }

  /**
   * Returns the name of the mechanism
   */
  get name(): string | null {
    return null;
  }

  /**
   * Set the options for the mechanism
   * @param {object} options Options specific to the mechanism
   */
  setopts(options: SaslMechanismBaseOptions) {
    this.options = options;
  }

  /**
   * Evaluates the challenge from the server and returns appropriate response
   * @param {String} challenge Challenge string presented by the server
   */
  abstract evaluateChallenge(challenge: String): any;
}
