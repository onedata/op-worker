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
props_to_value(od_user, Props) ->
    #od_user{
        name = proplists:get_value(<<"name">>, Props),
        default_space = case proplists:get_value(<<"default_space">>, Props) of
            <<"undefined">> -> undefined;
            Value -> Value
        end,
        space_aliases = proplists:get_value(<<"space_aliases">>, Props, []),
        groups = proplists:get_value(<<"groups">>, Props, []),
        handle_services = proplists:get_value(<<"handle_services">>, Props, []),
        handles = proplists:get_value(<<"handles">>, Props, []),
        public_only = proplists:get_value(<<"public_only">>, Props)
    };
props_to_value(od_group, Props) ->
    #od_group{
        name = proplists:get_value(<<"name">>, Props),
        type = binary_to_atom(proplists:get_value(<<"type">>, Props), utf8),
        users = process_ids_with_privileges(proplists:get_value(<<"users">>, Props, [])),
        eff_users = process_ids_with_privileges(
            proplists:get_value(<<"eff_users">>, Props, [])),
        children = process_ids_with_privileges(
            proplists:get_value(<<"children">>, Props, [])),
        parents = proplists:get_value(<<"parents">>, Props, []),
        spaces = proplists:get_value(<<"spaces">>, Props, []),
        handle_services = proplists:get_value(<<"handle_services">>, Props, []),
        handles = proplists:get_value(<<"handles">>, Props, [])
    };
props_to_value(od_space, Props) ->
    #od_space{
        name = proplists:get_value(<<"name">>, Props),
        providers_supports = proplists:get_value(<<"providers_supports">>, Props),
        groups = process_ids_with_privileges(proplists:get_value(<<"groups">>, Props, [])),
        users = process_ids_with_privileges(proplists:get_value(<<"users">>, Props, [])),
        shares = proplists:get_value(<<"shares">>, Props)
    };
props_to_value(od_share, Props) ->
    #od_share{
        name = proplists:get_value(<<"name">>, Props),
        public_url = proplists:get_value(<<"public_url">>, Props),
        root_file_id = proplists:get_value(<<"root_file_id">>, Props),
        parent_space = proplists:get_value(<<"parent_space">>, Props),
        handle = proplists:get_value(<<"handle">>, Props)
    };
props_to_value(od_provider, Props) ->
    #od_provider{
        client_name = proplists:get_value(<<"client_name">>, Props),
        urls = proplists:get_value(<<"urls">>, Props),
        space_ids = proplists:get_value(<<"space_ids">>, Props),
        public_only = proplists:get_value(<<"public_only">>, Props)
    };
props_to_value(od_handle_service, Props) ->
    #od_handle_service{
        name = proplists:get_value(<<"name">>, Props),
        proxy_endpoint = proplists:get_value(<<"proxy_endpoint">>, Props),
        service_properties = proplists:get_value(<<"service_properties">>, Props),
        users = process_ids_with_privileges(
            proplists:get_value(<<"users">>, Props, [])),
        groups = process_ids_with_privileges(
            proplists:get_value(<<"groups">>, Props, []))
    };
props_to_value(od_handle, Props) ->
    #od_handle{
        handle_service_id = proplists:get_value(<<"handle_service_id">>, Props),
        public_handle = proplists:get_value(<<"public_handle">>, Props),
        resource_type = proplists:get_value(<<"resource_type">>, Props),
        resource_id = proplists:get_value(<<"resource_id">>, Props),
        metadata = proplists:get_value(<<"metadata">>, Props),
        users = process_ids_with_privileges(
            proplists:get_value(<<"users">>, Props, [])),
        groups = process_ids_with_privileges(
            proplists:get_value(<<"groups">>, Props, [])),
        timestamp = deserialize_timestamp(
            proplists:get_value(<<"timestamp">>, Props))
    }.


%%--------------------------------------------------------------------
%% @doc @private
%% Translates model name from the json to actual model module.
%% @end
%%--------------------------------------------------------------------
-spec type_to_model(ModelRaw :: binary()) -> subscriptions:model().
type_to_model(<<"od_provider">>) -> od_provider;
type_to_model(<<"od_space">>) -> od_space;
type_to_model(<<"od_share">>) -> od_share;
type_to_model(<<"od_group">>) -> od_group;
type_to_model(<<"od_user">>) -> od_user;
type_to_model(<<"od_handle_service">>) -> od_handle_service;
type_to_model(<<"od_handle">>) -> od_handle;
type_to_model(_Type) -> ?error("Unexpected update type ~p", [_Type]).


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


%%-------------------------------------------------------------------
%% @doc
%% @private
%% Translates list of integers that come from subscriptions into
%% erlang datetime format.
%% @end
%%-------------------------------------------------------------------
-spec deserialize_timestamp([integer()]) -> calendar:datetime().
deserialize_timestamp([A, B, C, D, E, F]) ->
    {{A, B, C}, {D, E, F}}.
