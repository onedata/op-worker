%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019-2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles middleware operations (update) corresponding to file aspects such as:
%%% - attributes,
%%% - extended attributes,
%%% - json metadata,
%%% - rdf metadata.
%%% @end
%%%-------------------------------------------------------------------
-module(file_middleware_plugin_update).
-author("Bartosz Walkowicz").

-behaviour(middleware_handler).

-include("middleware/middleware.hrl").
-include("modules/logical_file_manager/lfm.hrl").


%% middleware_router callbacks
-export([resolve_handler/2]).

%% middleware_handler callbacks
-export([data_spec/1, fetch_entity/1, authorize/2, validate/2]).
-export([update/1]).


-spec resolve_handler(gri:aspect(), middleware:scope()) ->
    module() | no_return().
resolve_handler(instance, private) -> ?MODULE;              % gs only
resolve_handler(acl, private)      -> ?MODULE;
resolve_handler(_, _)              -> throw(?ERROR_NOT_SUPPORTED).

%%%===================================================================
%%% middleware_handler callbacks
%%%===================================================================

-spec data_spec(middleware:req()) -> undefined | middleware_sanitizer:data_spec().
data_spec(#op_req{gri = #gri{aspect = instance}}) ->
    ModeParam = <<"posixPermissions">>,
    
    #{required => #{
        id => {binary, guid},
        ModeParam => {binary, fun(Mode) ->
            try binary_to_integer(Mode, 8) of
                ValidMode when ValidMode >= 0 andalso ValidMode =< 8#777 ->
                    {true, ValidMode};
                _ ->
                    throw(?ERROR_BAD_VALUE_NOT_IN_RANGE(ModeParam, 0, 8#777))
            catch _:_ ->
                throw(?ERROR_BAD_VALUE_INTEGER(ModeParam))
            end
        end}
    }};

data_spec(#op_req{gri = #gri{aspect = acl}}) -> #{
    required => #{
        id => {binary, guid},
        <<"list">> => {any, fun(JsonAcl) ->
            try
                {true, acl:from_json(JsonAcl, gui)}
            catch throw:{error, Errno} ->
                throw(?ERROR_POSIX(Errno))
            end
        end}
    }
}.


-spec fetch_entity(middleware:req()) -> {ok, middleware:versioned_entity()}.
fetch_entity(_) ->
    {ok, {undefined, 1}}.


-spec authorize(middleware:req(), middleware:entity()) -> boolean().
authorize(#op_req{auth = Auth, gri = #gri{id = Guid, aspect = As}}, _) when
    As =:= instance;
    As =:= acl
    ->
    middleware_utils:has_access_to_file_space(Auth, Guid).


-spec validate(middleware:req(), middleware:entity()) -> ok | no_return().
validate(#op_req{gri = #gri{id = Guid, aspect = As}}, _) when
    As =:= instance;
    As =:= acl
    ->
    middleware_utils:assert_file_managed_locally(Guid).


-spec update(middleware:req()) -> middleware:update_result().
update(#op_req{auth = Auth, data = Data, gri = #gri{id = Guid, aspect = instance}}) ->
    case maps:get(<<"posixPermissions">>, Data, undefined) of
        undefined ->
            ok;
        PosixPerms ->
            ?lfm_check(lfm:set_perms(Auth#auth.session_id, ?FILE_REF(Guid), PosixPerms))
    end;
update(#op_req{auth = Auth, data = Data, gri = #gri{id = Guid, aspect = acl}}) ->
    ?lfm_check(lfm:set_acl(
        Auth#auth.session_id,
        ?FILE_REF(Guid),
        maps:get(<<"list">>, Data)
    )).
