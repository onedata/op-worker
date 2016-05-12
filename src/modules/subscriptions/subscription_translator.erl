%%%--------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Encodes raw json updates and outputs proper update records.
%%% @end
%%%--------------------------------------------------------------------
-module(subscription_translator).
-author("Michal Zmuda").

-include("global_definitions.hrl").
-include("proto/common/credentials.hrl").
-include("modules/subscriptions/subscriptions.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([json_to_updates/1]).

%%--------------------------------------------------------------------
%% @doc
%% Translates json update batch from OZ to tuples with update data.
%% @end
%%--------------------------------------------------------------------
-spec json_to_updates(RawJson :: binary()) -> [#sub_update{}].

json_to_updates(Raw) ->
    Json = json_utils:decode(Raw),
    lists:map(fun(Update) ->
        Seq = proplists:get_value(<<"seq">>, Update),
        Update1 = proplists:delete(<<"seq">>, Update),

        ID = proplists:get_value(<<"id">>, Update1),
        Update2 = proplists:delete(<<"id">>, Update1),

        Revs = proplists:get_value(<<"revs">>, Update2),
        Update3 = proplists:delete(<<"revs">>, Update2),

        Data = hd(Update3),
        case Data of
            {<<"ignore">>, true} ->
                #sub_update{ignore = true, seq = Seq};
            {ModelRaw, <<"delete">>} ->
                #sub_update{delete = true, seq = Seq, id = ID,
                    model = type_to_model(ModelRaw)};
            {ModelRaw, Props} ->
                Model = type_to_model(ModelRaw),
                Value = props_to_value(Model, Props),
                #sub_update{
                    seq = Seq, id = ID, revs = Revs, model = Model,
                    doc = #document{key = ID, value = Value, rev = hd(Revs)}
                };
            _ ->
                ?warning("Ignoring update data: ~p, seq: ~p", [Data, Seq]),
                #sub_update{ignore = true, seq = Seq}
        end
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
    Value :: subscriptions:record().
props_to_value(onedata_user, Props) ->
    #onedata_user{
        default_space = case proplists:get_value(<<"default_space">>, Props) of
            <<"undefined">> -> undefined;
            Value -> Value
        end,
        public_only = proplists:get_value(<<"public_only">>, Props),
        name = proplists:get_value(<<"name">>, Props),
        group_ids = proplists:get_value(<<"group_ids">>, Props, []),
        effective_group_ids = proplists:get_value(<<"effective_group_ids">>, Props, []),
        spaces = proplists:get_value(<<"space_names">>, Props, [])
    };
props_to_value(onedata_group, Props) ->
    #onedata_group{
        name = proplists:get_value(<<"name">>, Props),
        spaces = proplists:get_value(<<"spaces">>, Props, []),
        users = process_ids_with_privileges(proplists:get_value(<<"users">>, Props, [])),
        effective_users = process_ids_with_privileges(
            proplists:get_value(<<"effective_users">>, Props, [])),
        nested_groups = process_ids_with_privileges(
            proplists:get_value(<<"nested_groups">>, Props, [])),
        parent_groups = proplists:get_value(<<"parent_groups">>, Props, [])
    };
props_to_value(space_info, Props) ->
    #space_info{
        name = proplists:get_value(<<"name">>, Props),
        providers_supports = proplists:get_value(<<"providers_supports">>, Props),
        groups = process_ids_with_privileges(proplists:get_value(<<"groups">>, Props, [])),
        users = process_ids_with_privileges(proplists:get_value(<<"users">>, Props, []))
    };
props_to_value(provider_info, Props) ->
    #provider_info{
        client_name = proplists:get_value(<<"client_name">>, Props)
    }.


%%--------------------------------------------------------------------
%% @doc @private
%% Translates model name from the json to actual model module.
%% @end
%%--------------------------------------------------------------------
-spec type_to_model(ModelRaw :: binary()) -> subscriptions:model().
type_to_model(<<"provider">>) ->
    provider_info;
type_to_model(<<"space">>) ->
    space_info;
type_to_model(<<"group">>) ->
    onedata_group;
type_to_model(<<"user">>) ->
    onedata_user;
type_to_model(_Type) ->
    ?error("Unexpected update type ~p", [_Type]).


%%--------------------------------------------------------------------
%% @doc @private
%% For each ID converts binary privileges to atoms.
%% @end
%%--------------------------------------------------------------------
-spec process_ids_with_privileges([{ID :: binary(), Privileges :: [binary()]}]) ->
    [{ID1 :: binary(), Privileges1 :: [atom()]}].
process_ids_with_privileges(Raw) ->
    lists:map(fun({ID, Binaries}) ->
        {ID, lists:map(fun(Bin) ->
            binary_to_atom(Bin, latin1)
        end, Binaries)}
    end, Raw).