/*   Copyright (C) 2013-2015 Computer Sciences Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. */

package ezbake.intent.query.utils;

import ezbake.query.intents.BinaryOperator;
import ezbake.query.intents.ColumnValue;

import java.util.UUID;

public class Conversions {

    public static String convertOperatorToString(BinaryOperator binoperator) {
        String retVal = null;

        int value = binoperator.getValue();

        switch (value) {

            case 0:
                retVal = "<";
                break;

            case 1:
                retVal = "<=";
                break;

            case 2:
                retVal = "=";
                break;

            case 3:
                retVal = "!=";
                break;

            case 4:
                retVal = ">=";
                break;

            case 5:
                retVal = ">";
                break;
        }
        return retVal;
    }

    public static Object convertColValueToObject(ColumnValue colval) {
        Object obj = null;

        if (colval.isSet(1)) {
            obj = colval.getBoolValue();
        } else if (colval.isSet(2)) {
            obj = colval.getIntegerValue();
        } else if (colval.isSet(3)) {
            obj = colval.getLongValue();
        } else if (colval.isSet(4)) {
            obj = colval.getDoubleValue();
        } else if (colval.isSet(5)) {
            // TODO escape the string value
            obj = "'" + colval.getStringValue() + "'";
        }

        return obj;
    }

    public static String generateUUID() {
        String uuid = "";

        UUID idOne = UUID.randomUUID();
        uuid = String.valueOf(idOne);

        return uuid;
    }

}
