package graphql.execution;

import graphql.ExecutionResult;
import graphql.ExecutionResultImpl;
import graphql.GraphQLException;
import graphql.execution.instrumentation.Instrumentation;
import graphql.execution.instrumentation.InstrumentationContext;
import graphql.execution.instrumentation.parameters.FieldFetchParameters;
import graphql.execution.instrumentation.parameters.FieldParameters;
import graphql.language.Field;
import graphql.schema.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static graphql.execution.TypeInfo.newTypeInfo;

public class AsynchronousExecutionStrategy extends ExecutionStrategy {

    private static final Logger log = LoggerFactory.getLogger(AsynchronousExecutionStrategy.class);

    @Override
    public ExecutionResult execute(ExecutionContext executionContext,
                    ExecutionParameters parameters) throws NonNullableFieldWasNullException {
        Map<String, List<Field>> fields = parameters.fields();
        Map<String, Future<ExecutionResult>> futures = new LinkedHashMap<>();
        for (String fieldName : fields.keySet()) {
            final List<Field> fieldList = fields.get(fieldName);
            Future<ExecutionResult> resolveField = resolveFieldAsync(executionContext, parameters, fieldList);
            futures.put(fieldName, resolveField);
        }
        try {
            Map<String, Object> results = new LinkedHashMap<>();
            for (String fieldName : futures.keySet()) {
                ExecutionResult executionResult = futures.get(fieldName).get();

                results.put(fieldName, executionResult != null ? executionResult.getData() : null);
            }
            return new ExecutionResultImpl(results, executionContext.getErrors());
        } catch (InterruptedException e) {
            throw new GraphQLException(e);
        } catch (ExecutionException e) {
            throw new GraphQLException(e);
        }
    }

    protected Future<ExecutionResult> resolveFieldAsync(ExecutionContext executionContext, ExecutionParameters parameters, List<Field> fields) {
        GraphQLObjectType type = parameters.typeInfo().castType(GraphQLObjectType.class);
        GraphQLFieldDefinition
                        fieldDef = getFieldDef(executionContext.getGraphQLSchema(), type, fields.get(0));

        Map<String, Object> argumentValues = valuesResolver.getArgumentValues(fieldDef.getArguments(), fields.get(0).getArguments(), executionContext.getVariables());

        GraphQLOutputType fieldType = fieldDef.getType();
        DataFetchingFieldSelectionSet fieldCollector = DataFetchingFieldSelectionSetImpl
                        .newCollector(executionContext, fieldType, fields);

        DataFetchingEnvironment environment = new DataFetchingEnvironmentImpl(
                        parameters.source(),
                        argumentValues,
                        executionContext.getRoot(),
                        fields,
                        fieldType,
                        type,
                        executionContext.getGraphQLSchema(),
                        executionContext.getFragmentsByName(),
                        executionContext.getExecutionId(),
                        fieldCollector);

        Instrumentation instrumentation = executionContext.getInstrumentation();

        InstrumentationContext<ExecutionResult>
                        fieldCtx = instrumentation.beginField(new FieldParameters(executionContext, fieldDef, environment));

        InstrumentationContext<Object> fetchCtx = instrumentation.beginFieldFetch(new FieldFetchParameters(executionContext, fieldDef, environment));
        Object resolvedValue = null;

        CompletableFuture<Object> dataFetcherResult = null;
        try {
            resolvedValue = fieldDef.getDataFetcher().get(environment);

            if(resolvedValue instanceof CompletableFuture) {
                dataFetcherResult = (CompletableFuture) resolvedValue;
            } else {
                dataFetcherResult = CompletableFuture.completedFuture(resolvedValue);
            }
        } catch (Exception e) {
            log.warn("Exception while fetching data", e);
            dataFetcherResult = new CompletableFuture();
            dataFetcherResult.completeExceptionally(e);
        }

        return dataFetcherResult.handle((value,th)-> {
            if(th != null) {
                log.warn("Exception while fetching data", th);
                handleDataFetchingException(executionContext, fieldDef, argumentValues, th);
                fetchCtx.onEnd(th);
            }

            TypeInfo fieldTypeInfo = newTypeInfo()
                            .type(fieldType)
                            .parentInfo(parameters.typeInfo())
                            .build();


            ExecutionParameters newParameters = ExecutionParameters.newParameters()
                            .typeInfo(fieldTypeInfo)
                            .fields(parameters.fields())
                            .arguments(argumentValues)
                            .source(value).build();

            ExecutionResult result = completeValue(executionContext, newParameters, fields);

            return result;
        });


    }
}
