package graphql.execution

import graphql.GraphQL
import graphql.schema.GraphQLObjectType
import graphql.schema.GraphQLSchema
import spock.lang.Specification

import java.util.concurrent.CompletableFuture

import static graphql.Scalars.GraphQLString
import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition
import static graphql.schema.GraphQLObjectType.newObject

class AsynchronousExecutionStrategyTest extends Specification {

    def "Example usage of AsynchronousExecutionStrategy."() {
        given:

        GraphQLObjectType queryType = newObject()
                .name("data")
                .field(newFieldDefinition()
                    .type(GraphQLString)
                    .name("key1")
                    .dataFetcher({env -> CompletableFuture.completedFuture("value1")}))
                .field(newFieldDefinition()
                    .type(GraphQLString)
                    .name("key2")
                    .staticValue("value2"))
                .build();

        GraphQLSchema schema = GraphQLSchema.newSchema()
                .query(queryType)
                .build();

        def expected = [key1:"value1",key2:"value2"]

        when:
        GraphQL graphQL = GraphQL.newGraphQL(schema)
                .queryExecutionStrategy(new AsynchronousExecutionStrategy())
                .build();

        Map<String, Object> result = graphQL.execute("{key1,key2}").getData();

        then:

        assert expected == result
    }
}
