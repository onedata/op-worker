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
-module(file_middleware_update_handler).
-author("Bartosz Walkowicz").

-behaviour(middleware_handler).

-include("middleware/middleware.hrl").
-include("modules/logical_file_manager/lfm.hrl").


%% middleware_router callbacks
-export([assert_operation_supported/2]).

%% middleware_handler callbacks
-export([data_spec/1, fetch_entity/1, authorize/2, validate/2]).
-export([create/1, get/2, update/1, delete/1]).


%%%===================================================================
%%% API
%%%===================================================================

-spec assert_operation_supported(gri:aspect(), middleware:scope()) ->
    ok | no_return().
assert_operation_supported(instance, private) -> ok;              % gs only
assert_operation_supported(acl, private)      -> ok;
assert_operation_supported(_, _)              -> throw(?ERROR_NOT_SUPPORTED).


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


%% @doc {@link middleware_handler} callback create/1.
-spec create(middleware:req()) -> middleware:create_result().
create(_) ->
    error(not_implemented).


%% @doc {@link middleware_handler} callback get/2.
-spec get(middleware:req(), middleware:entity()) -> middleware:get_result().
get(_, _) ->
    error(not_implemented).


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


%% @doc {@link middleware_handler} callback delete/1.
-spec delete(middleware:req()) -> middleware:delete_result().
delete(_) ->
    error(not_implemented).