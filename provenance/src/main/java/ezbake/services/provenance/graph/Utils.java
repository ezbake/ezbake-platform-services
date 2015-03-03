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

package ezbake.services.provenance.graph;

import ezbake.base.thrift.TokenType;
import ezbake.base.thrift.DateTime;
import ezbake.data.common.DateTimeConverter;
import ezbake.security.client.EzSecurityTokenWrapper;
import org.apache.commons.lang.StringUtils;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class Utils {

    // Get current date time ignore milliseconds
    public static Date getCurrentDate() {
        // zero out millisecond
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTime();
    }

    // Convert thrift DateTime to java Date
    public static Date convertDateTime2Date(final ezbake.base.thrift.DateTime dateTime) {
        Calendar calendar = DateTimeConverter.transformDateTime(dateTime);
        return calendar.getTime();
    }

    public static Long convertDateTime2Millis(final DateTime dateTime){
        Calendar calendar = DateTimeConverter.transformDateTime(dateTime);
        return calendar.getTimeInMillis();
    }

    // Convert java Date to thrift DateTime
    public static ezbake.base.thrift.DateTime convertDate2DateTime(final java.util.Date theDate) {
        final Calendar cal = new GregorianCalendar();
        cal.setTime(theDate);

        // get calendar parts
        final int year = cal.get(Calendar.YEAR);
        final int month = cal.get(Calendar.MONTH) + 1;
        final int day = cal.get(Calendar.DAY_OF_MONTH);
        final int hour = cal.get(Calendar.HOUR_OF_DAY);
        final int minute = cal.get(Calendar.MINUTE);
        final int second = cal.get(Calendar.SECOND);
        int offsetMinutes = (cal.getTimeZone().getOffset(cal.getTimeInMillis())) / (1000 * 60);

        // set thrift DateTime propertiesd
        final ezbake.base.thrift.DateTime dt = new ezbake.base.thrift.DateTime();
        // Date
        final ezbake.base.thrift.Date date = new ezbake.base.thrift.Date();
        date.setMonth((short) month).setDay((short) day).setYear((short) year);
        dt.setDate(date);

        // Time with TimeZone
        final ezbake.base.thrift.Time t = new ezbake.base.thrift.Time();
        boolean afterUtc = offsetMinutes > 0;
        offsetMinutes = Math.abs(offsetMinutes);
        final ezbake.base.thrift.TimeZone tz = new ezbake.base.thrift.TimeZone((short) (offsetMinutes / 60), (short) (offsetMinutes % 60), afterUtc);
        t.setHour((short) hour).setMinute((short) minute).setSecond((short) second).setTz(tz);
        dt.setTime(t);

        return dt;
    }

    // Get app from security token
    public static String getApplication(final ezbake.base.thrift.EzSecurityToken token) {
        EzSecurityTokenWrapper wrapper = new EzSecurityTokenWrapper(token);
        String app = wrapper.getSecurityId();
        if (StringUtils.isEmpty(app)) {
            app = "";
        }

        return app;
    }

    // Get user from security token
    public static String getUser(final ezbake.base.thrift.EzSecurityToken token) {
        String user = "";
        if(token.getType()== TokenType.USER) {
            user = token.getTokenPrincipal().getPrincipal();
        }
        else {
            EzSecurityTokenWrapper wrapper = new EzSecurityTokenWrapper(token);
            user = wrapper.getSecurityId();
        }

        if (StringUtils.isEmpty(user)) {
            user = "NOT SET";
        }
        return user;

    }

    // True if app is the central purge
    public static boolean isAdminApplication(ezbake.base.thrift.EzSecurityToken token, String adminApp) {
        return getApplication(token).equals(adminApp);
    }

    // True if app is central purge or the original app created the record
    public static boolean isAuthenticatedToUpdate(ezbake.base.thrift.EzSecurityToken token, String app, String adminApp) {
        String application = getApplication(token);
        return application.equals(app) || application.equals(adminApp);
    }

}
