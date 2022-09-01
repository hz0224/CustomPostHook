//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package hooks;

import com.google.common.collect.Lists;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.gson.stream.JsonWriter;
import org.apache.commons.collections.SetUtils;
import org.apache.commons.io.output.StringBuilderWriter;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TaskRunner;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext.HookType;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.BaseColumnInfo;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.Dependency;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.Predicate;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.lineage.LineageCtx.Index;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.ql.tools.LineageInfo;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.*;

public class CustomLineageToMysql implements ExecuteWithHookContext {
    private static final Logger LOG = LoggerFactory.getLogger(CustomLineageToMysql.class);
    private static final HashSet<String> OPERATION_NAMES = new HashSet();
    private static final String FORMAT_VERSION = "1.0";
    private static final String URL = "jdbc:mysql://yun:3306/test";
    private static final String DRIVER ="com.mysql.jdbc.Driver";
    private static final String USER = "root";
    private static final String PASSWORD = "17746371311";

    public CustomLineageToMysql() {
    }

    public void run(HookContext hookContext) {
        assert hookContext.getHookType() == HookType.POST_EXEC_HOOK;

        QueryPlan plan = hookContext.getQueryPlan();
        Index index = hookContext.getIndex();
        SessionState ss = SessionState.get();
        if (ss != null && index != null && OPERATION_NAMES.contains(plan.getOperationName()) && !plan.isExplain()) {
            try {
                StringBuilderWriter out = new StringBuilderWriter(1024);
                JsonWriter writer = new JsonWriter(out);
                String queryStr = plan.getQueryStr().trim();
                writer.beginObject();
                writer.name("version").value("1.0");
                HiveConf conf = ss.getConf();
                boolean testMode = conf.getBoolVar(ConfVars.HIVE_IN_TEST);
                if (!testMode) {
                    long queryTime = plan.getQueryStartTime();
                    if (queryTime == 0L) {
                        queryTime = System.currentTimeMillis();
                    }

                    long duration = System.currentTimeMillis() - queryTime;
                    writer.name("user").value(hookContext.getUgi().getUserName());
                    writer.name("timestamp").value(queryTime / 1000L);
                    writer.name("duration").value(duration);
                    writer.name("jobIds");
                    writer.beginArray();
                    List<TaskRunner> tasks = hookContext.getCompleteTaskList();
                    if (tasks != null && !tasks.isEmpty()) {
                        Iterator var15 = tasks.iterator();

                        while(var15.hasNext()) {
                            TaskRunner task = (TaskRunner)var15.next();
                            String jobId = task.getTask().getJobID();
                            if (jobId != null) {
                                writer.value(jobId);
                            }
                        }
                    }

                    writer.endArray();
                }

                writer.name("engine").value(HiveConf.getVar(conf, ConfVars.HIVE_EXECUTION_ENGINE));
                writer.name("database").value(ss.getCurrentDatabase());
                writer.name("hash").value(this.getQueryHash(queryStr));
                writer.name("queryText").value(queryStr);

                //增加对输出表的判断.
                LineageInfo lep = new LineageInfo();
                lep.getLineageInfo(queryStr);
                Iterator var3 = lep.getInputTableList().iterator();
                HashSet<String> inputTables = new HashSet<>();
                HashSet<String> outputTables = new HashSet<>();
                while(var3.hasNext()) {
                    String inputTable = (String)var3.next();
                    inputTables.add(inputTable);
                }
                var3 = lep.getOutputTableList().iterator();

                while(var3.hasNext()) {
                    String outputTable = (String)var3.next();
                    outputTables.add(outputTable);
                }

                writer.name("input_tables").value(String.join(",", inputTables));
                writer.name("output_tables").value(String.join(",", outputTables));

                List<Edge> edges = this.getEdges(plan, index);
                Set<Vertex> vertices = this.getVertices(edges);
                this.writeEdges(writer, edges);
                this.writeVertices(writer, vertices);
                writer.endObject();
                writer.close();
                String lineage = out.toString();
                if (testMode) {
                    this.log(lineage);
                } else {
                    //有输出表时再进行输出.
                    if(outputTables.size() > 0){
                        //写mysql
                        Class.forName(DRIVER);
                        Connection connection = DriverManager.getConnection(URL, USER, PASSWORD);
                        PreparedStatement ps = connection.prepareStatement("insert into test.lineage (`val`) values (?)");
                        ps.setObject(1, lineage);
                        ps.executeUpdate();
                        ps.close();
                        connection.close();
                    }
                }
            } catch (Throwable var21) {
                this.log("Failed to log lineage graph, query is not affected\n" + StringUtils.stringifyException(var21));
            }
        }

    }

    private void log(String error) {
        LogHelper console = SessionState.getConsole();
        if (console != null) {
            console.printError(error);
        }

    }

    private List<Edge> getEdges(QueryPlan plan, Index index) {
        LinkedHashMap<String, ObjectPair<SelectOperator, Table>> finalSelOps = index.getFinalSelectOps();
        Map<String, Vertex> vertexCache = new LinkedHashMap();
        List<Edge> edges = new ArrayList();
        Iterator var6 = finalSelOps.values().iterator();

        while(true) {
            LinkedHashSet targets;
            Set conds;
            do {
                label90:
                do {
                    while(var6.hasNext()) {
                        ObjectPair<SelectOperator, Table> pair = (ObjectPair)var6.next();
                        List<FieldSchema> fieldSchemas = plan.getResultSchema().getFieldSchemas();
                        SelectOperator finalSelOp = (SelectOperator)pair.getFirst();
                        Table t = (Table)pair.getSecond();
                        String destTableName = null;
                        List<String> colNames = null;
                        List partitionKeys;
                        if (t != null) {
                            destTableName = t.getDbName() + "." + t.getTableName();
                            fieldSchemas = t.getCols();
                        } else {
                            label110: {
                                Iterator var13 = plan.getOutputs().iterator();

                                WriteEntity output;
                                Entity.Type entityType;
                                do {
                                    if (!var13.hasNext()) {
                                        break label110;
                                    }

                                    output = (WriteEntity)var13.next();
                                    entityType = output.getType();
                                } while(entityType != Entity.Type.TABLE && entityType != Entity.Type.PARTITION);

                                t = output.getTable();
                                destTableName = t.getDbName() + "." + t.getTableName();
                                partitionKeys = t.getCols();
                                if (partitionKeys != null && !partitionKeys.isEmpty()) {
                                    colNames = Utilities.getColumnNamesFromFieldSchema(partitionKeys);
                                }
                            }
                        }

                        Map<ColumnInfo, Dependency> colMap = index.getDependencies(finalSelOp);
                        List<Dependency> dependencies = colMap != null ? Lists.newArrayList(colMap.values()) : null;
                        int fields = fieldSchemas.size();
                        int i;
                        if (t != null && colMap != null && fields < colMap.size()) {
                            partitionKeys = t.getPartitionKeys();
                            i = colMap.size() - fields;
                            int keyOffset = partitionKeys.size() - i;
                            if (keyOffset >= 0) {
                                fields += i;

                                for(int j = 0; j < i; ++j) {
                                    FieldSchema field = (FieldSchema)partitionKeys.get(keyOffset + j);
                                    fieldSchemas.add(field);
                                    if (colNames != null) {
                                        colNames.add(field.getName());
                                    }
                                }
                            }
                        }

                        if (dependencies != null && dependencies.size() == fields) {
                            targets = new LinkedHashSet();

                            for(i = 0; i < fields; ++i) {
                                Vertex target = this.getOrCreateVertex(vertexCache, this.getTargetFieldName(i, destTableName, colNames, fieldSchemas), Vertex.Type.COLUMN);
                                targets.add(target);
                                Dependency dep = (Dependency)dependencies.get(i);
                                this.addEdge(vertexCache, edges, dep.getBaseCols(), (Vertex)target, dep.getExpr(), Edge.Type.PROJECTION);
                            }

                            conds = index.getPredicates(finalSelOp);
                            continue label90;
                        }

                        this.log("Result schema has " + fields + " fields, but we don't get as many dependencies");
                    }

                    return edges;
                } while(conds == null);
            } while(conds.isEmpty());

            Iterator var27 = conds.iterator();

            while(var27.hasNext()) {
                Predicate cond = (Predicate)var27.next();
                this.addEdge(vertexCache, edges, cond.getBaseCols(), (Set)(new LinkedHashSet(targets)), cond.getExpr(), Edge.Type.PREDICATE);
            }
        }
    }

    private void addEdge(Map<String, Vertex> vertexCache, List<Edge> edges, Set<BaseColumnInfo> srcCols, Vertex target, String expr, Edge.Type type) {
        Set<Vertex> targets = new LinkedHashSet();
        targets.add(target);
        this.addEdge(vertexCache, edges, srcCols, (Set)targets, expr, type);
    }

    private void addEdge(Map<String, Vertex> vertexCache, List<Edge> edges, Set<BaseColumnInfo> srcCols, Set<Vertex> targets, String expr, Edge.Type type) {
        Set<Vertex> sources = this.createSourceVertices(vertexCache, srcCols);
        Edge edge = this.findSimilarEdgeBySources(edges, sources, expr, type);
        if (edge == null) {
            edges.add(new Edge(sources, targets, expr, type));
        } else {
            edge.targets.addAll(targets);
        }

    }

    private Set<Vertex> createSourceVertices(Map<String, Vertex> vertexCache, Collection<BaseColumnInfo> baseCols) {
        Set<Vertex> sources = new LinkedHashSet();
        if (baseCols != null && !baseCols.isEmpty()) {
            Iterator var4 = baseCols.iterator();

            while(var4.hasNext()) {
                BaseColumnInfo col = (BaseColumnInfo)var4.next();
                org.apache.hadoop.hive.metastore.api.Table table = col.getTabAlias().getTable();
                if (!table.isTemporary()) {
                    Vertex.Type type = Vertex.Type.TABLE;
                    String tableName = table.getDbName() + "." + table.getTableName();
                    FieldSchema fieldSchema = col.getColumn();
                    String label = tableName;
                    if (fieldSchema != null) {
                        type = Vertex.Type.COLUMN;
                        label = tableName + "." + fieldSchema.getName();
                    }

                    sources.add(this.getOrCreateVertex(vertexCache, label, type));
                }
            }
        }

        return sources;
    }

    private Vertex getOrCreateVertex(Map<String, Vertex> vertices, String label, Vertex.Type type) {
        Vertex vertex = (Vertex)vertices.get(label);
        if (vertex == null) {
            vertex = new Vertex(label, type);
            vertices.put(label, vertex);
        }

        return vertex;
    }

    private Edge findSimilarEdgeBySources(List<Edge> edges, Set<Vertex> sources, String expr, Edge.Type type) {
        Iterator var5 = edges.iterator();

        Edge edge;
        do {
            if (!var5.hasNext()) {
                return null;
            }

            edge = (Edge)var5.next();
        } while(edge.type != type || !org.apache.commons.lang.StringUtils.equals(edge.expr, expr) || !SetUtils.isEqualSet(edge.sources, sources));

        return edge;
    }

    private String getTargetFieldName(int fieldIndex, String destTableName, List<String> colNames, List<FieldSchema> fieldSchemas) {
        String fieldName = ((FieldSchema)fieldSchemas.get(fieldIndex)).getName();
        String[] parts = fieldName.split("\\.");
        if (destTableName != null) {
            String colName = parts[parts.length - 1];
            if (colNames != null && !colNames.contains(colName)) {
                colName = (String)colNames.get(fieldIndex);
            }

            return destTableName + "." + colName;
        } else {
            return parts.length == 2 && parts[0].startsWith("_u") ? parts[1] : fieldName;
        }
    }

    private Set<Vertex> getVertices(List<Edge> edges) {
        Set<Vertex> vertices = new LinkedHashSet();
        Iterator var3 = edges.iterator();

        Edge edge;
        while(var3.hasNext()) {
            edge = (Edge)var3.next();
            vertices.addAll(edge.targets);
        }

        var3 = edges.iterator();

        while(var3.hasNext()) {
            edge = (Edge)var3.next();
            vertices.addAll(edge.sources);
        }

        int id = 0;
        Iterator var7 = vertices.iterator();

        while(var7.hasNext()) {
            Vertex vertex = (Vertex)var7.next();
            vertex.id = id++;
        }

        return vertices;
    }

    private void writeEdges(JsonWriter writer, List<Edge> edges) throws IOException {
        writer.name("edges");
        writer.beginArray();
        Iterator var3 = edges.iterator();

        while(var3.hasNext()) {
            Edge edge = (Edge)var3.next();
            writer.beginObject();
            writer.name("sources");
            writer.beginArray();
            Iterator var5 = edge.sources.iterator();

            Vertex vertex;
            while(var5.hasNext()) {
                vertex = (Vertex)var5.next();
                writer.value((long)vertex.id);
            }

            writer.endArray();
            writer.name("targets");
            writer.beginArray();
            var5 = edge.targets.iterator();

            while(var5.hasNext()) {
                vertex = (Vertex)var5.next();
                writer.value((long)vertex.id);
            }

            writer.endArray();
            if (edge.expr != null) {
                writer.name("expression").value(edge.expr);
            }

            writer.name("edgeType").value(edge.type.name());
            writer.endObject();
        }

        writer.endArray();
    }

    private void writeVertices(JsonWriter writer, Set<Vertex> vertices) throws IOException {
        writer.name("vertices");
        writer.beginArray();
        Iterator var3 = vertices.iterator();

        while(var3.hasNext()) {
            Vertex vertex = (Vertex)var3.next();
            writer.beginObject();
            writer.name("id").value((long)vertex.id);
            writer.name("vertexType").value(vertex.type.name());
            writer.name("vertexId").value(vertex.label);
            writer.endObject();
        }

        writer.endArray();
    }

    private String getQueryHash(String queryStr) {
        Hasher hasher = Hashing.md5().newHasher();
        hasher.putString(queryStr);
        return hasher.hash().toString();
    }

    static {
        OPERATION_NAMES.add(HiveOperation.QUERY.getOperationName());
        OPERATION_NAMES.add(HiveOperation.CREATETABLE_AS_SELECT.getOperationName());
        OPERATION_NAMES.add(HiveOperation.ALTERVIEW_AS.getOperationName());
        OPERATION_NAMES.add(HiveOperation.CREATEVIEW.getOperationName());
    }

    static final class Vertex {
        private Type type;
        private String label;
        private int id;

        Vertex(String label) {
            this(label, Type.COLUMN);
        }

        Vertex(String label, Type type) {
            this.label = label;
            this.type = type;
        }

        public int hashCode() {
            return this.label.hashCode() + this.type.hashCode() * 3;
        }

        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            } else if (!(obj instanceof Vertex)) {
                return false;
            } else {
                Vertex vertex = (Vertex)obj;
                return this.label.equals(vertex.label) && this.type == vertex.type;
            }
        }

        public static enum Type {
            COLUMN,
            TABLE;

            private Type() {
            }
        }
    }

    static final class Edge {
        private Set<Vertex> sources;
        private Set<Vertex> targets;
        private String expr;
        private Type type;

        Edge(Set<Vertex> sources, Set<Vertex> targets, String expr, Type type) {
            this.sources = sources;
            this.targets = targets;
            this.expr = expr;
            this.type = type;
        }

        public static enum Type {
            PROJECTION,
            PREDICATE;

            private Type() {
            }
        }
    }
}
