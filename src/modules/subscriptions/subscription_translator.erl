%%%--------------------------------------------------------------------
%%% @author Michal Żmuda
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% @end
%%%--------------------------------------------------------------------
-module(subscription_translator).
-author("Michal Żmuda").

-include("global_definitions.hrl").
-include("proto/common/credentials.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([json_to_updates/1]).

%%--------------------------------------------------------------------
%% @doc
%% Translates json update batch from OZ to tuples with update data.
%% @end
%%--------------------------------------------------------------------

-spec json_to_updates(RawJson :: binary) -> [{
    Update :: datastore:document(),
    Model :: subscriptions:model(),
    Revisions :: [subscriptions:rev()],
    SequenceNumber :: subscriptions:seq()
}].

json_to_updates(Raw) ->
    Json = json_utils:decode(Raw),
    lists:map(fun(Update) ->
        Seq = proplists:get_value(<<"seq">>, Update),
        Update1 = proplists:delete(<<"seq">>, Update),

        ID = proplists:get_value(<<"id">>, Update1),
        Update2 = proplists:delete(<<"id">>, Update1),

        Revs = proplists:get_value(<<"revs">>, Update2),
        Update3 = proplists:delete(<<"revs">>, Update2),

        [Data] = Update3,
        ModelRaw = element(1, Data),
        Props = element(2, Data),

        Model = type_to_model(ModelRaw),
        Value = props_to_value(Model, Props),
        Rev = hd(Revs),
        Doc = #document{key = ID, value = Value, rev = Rev},

        {Doc, Model, Revs, Seq}
    end, Json).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc @private
%% Constructs value of the document from the proplist with values.
%% @end
%%--------------------------------------------------------------------
-spec props_to_value(Model :: subscriptions:model(), [{binary(), term()}]) ->
    Value :: term().

props_to_value(onedata_user, Props) ->
    #onedata_user{
        name = proplists:get_value(<<"name">>, Props),
        group_ids = proplists:get_value(<<"group_ids">>, Props),
        space_ids = proplists:get_value(<<"space_ids">>, Props)
    };
props_to_value(onedata_group, Props) ->
    #onedata_group{
        name = proplists:get_value(<<"name">>, Props)
    };
props_to_value(space_info, Props) ->
    #space_info{
        id = proplists:get_value(<<"id">>, Props),
        name = proplists:get_value(<<"name">>, Props)
    }.


%%--------------------------------------------------------------------
%% @doc @private
%% Translates model name from the json to actual model module.
%% @end
%%--------------------------------------------------------------------
-spec type_to_model(ModelRaw :: binary()) -> subscriptions:model().

type_to_model(<<"space">>) ->
    space_info;
type_to_model(<<"onedata_group">>) ->
    onedata_group;
type_to_model(<<"onedata_user">>) ->
    onedata_user;
type_to_model(_Type) ->
    ?error("Unexpected update type ~p", [_Type]).