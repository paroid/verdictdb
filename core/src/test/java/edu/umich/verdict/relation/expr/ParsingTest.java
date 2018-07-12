/*
 * Copyright 2017 University of Michigan
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
 * limitations under the License.
 */

package edu.umich.verdict.relation.expr;

import org.junit.Test;

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.Relation;
import edu.umich.verdict.relation.SingleRelation;

public class ParsingTest {

    @Test
    public void test() throws VerdictException {
        VerdictConf conf = new VerdictConf();
        conf.setDbms("dummy");
        VerdictJDBCContext vc = VerdictJDBCContext.from(conf);

        Relation r = SingleRelation.from(vc, "orders")
                .where(String.format("abs(fnv_hash(%s)) %% 10000 <= %.4f", "order_dow", 0.01 * 10000))
                .select("*, round(rand(unix_timestamp())*100)%100 AS " + "verdict_partition");
        String sql = r.toSql();

        System.out.println(sql);

        System.out.println(Relation.prettyfySql(vc, sql));
    }

}
