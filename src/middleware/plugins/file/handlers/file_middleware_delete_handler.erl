%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019-2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles middleware operations (delete)
%%% corresponding to file aspects such as:
%%% - attributes,
%%% - extended attributes,
%%% - json metadata,
%%% - rdf metadata.
%%% @end
%%%-------------------------------------------------------------------
-module(file_middleware_delete_handler).
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
assert_operation_supported(instance, private)      -> ok;        % gs only
assert_operation_supported(xattrs, private)        -> ok;        % REST/gs
assert_operation_supported(json_metadata, private) -> ok;        % REST/gs
assert_operation_supported(rdf_metadata, private)  -> ok;        % REST/gs
assert_operation_supported(_, _)                   -> throw(?ERROR_NOT_SUPPORTED).


%%%===================================================================
%%% middleware_handler callbacks
%%%===================================================================

-spec data_spec(middleware:req()) -> undefined | middleware_sanitizer:data_spec().
data_spec(#op_req{gri = #gri{aspect = instance}}) ->
    #{required => #{id => {binary, guid}}};

data_spec(#op_req{gri = #gri{aspect = As}}) when
    As =:= json_metadata;
    As =:= rdf_metadata
->
    #{
        required => #{id => {binary, guid}},
        optional => #{<<"resolve_symlink">> => {boolean, any}}
    };

data_spec(#op_req{gri = #gri{aspect = xattrs}}) -> #{
    required => #{
        id => {binary, guid},
        <<"keys">> => {list_of_binaries, any}
    },
    optional => #{
        <<"resolve_symlink">> => {boolean, any}
    }
}.


-spec fetch_entity(middleware:req()) -> {ok, middleware:versioned_entity()}.
fetch_entity(_) ->
    {ok, {undefined, 1}}.


-spec authorize(middleware:req(), middleware:entity()) -> boolean().
authorize(#op_req{auth = Auth, gri = #gri{id = Guid, aspect = As}}, _) when
    As =:= instance;
    As =:= xattrs;
    As =:= json_metadata;
    As =:= rdf_metadata
->
    middleware_utils:has_access_to_file_space(Auth, Guid).


-spec validate(middleware:req(), middleware:entity()) -> ok | no_return().
validate(#op_req{gri = #gri{id = Guid, aspect = As}}, _) when
    As =:= instance;
    As =:= xattrs;
    As =:= json_metadata;
    As =:= rdf_metadata
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


%% @doc {@link middleware_handler} callback update/1.
-spec update(middleware:req()) -> middleware:update_result().
update(_) ->
    error(not_implemented).


-spec delete(middleware:req()) -> middleware:delete_result().
delete(#op_req{auth = ?USER(_UserId, SessionId), gri = #gri{id = FileGuid, aspect = instance}}) ->
    FileRef = ?FILE_REF(FileGuid),
    
    case ?lfm_check(lfm:stat(SessionId, FileRef)) of
        {ok, #file_attr{type = ?DIRECTORY_TYPE}} ->
            ?lfm_check(lfm:rm_recursive(SessionId, FileRef));
        {ok, _} ->
            ?lfm_check(lfm:unlink(SessionId, FileRef, false))
    end;

delete(#op_req{auth = Auth, data = Data, gri = #gri{id = FileGuid, aspect = xattrs}}) ->
    FileRef = ?FILE_REF(FileGuid, maps:get(<<"resolve_symlink">>, Data, true)),
    
    lists:foreach(fun(XattrName) ->
        ?lfm_check(lfm:remove_xattr(Auth#auth.session_id, FileRef, XattrName))
    end, maps:get(<<"keys">>, Data));

delete(#op_req{auth = Auth, data = Data, gri = #gri{id = FileGuid, aspect = json_metadata}}) ->
    FileRef = ?FILE_REF(FileGuid, maps:get(<<"resolve_symlink">>, Data, true)),
    mi_file_metadata:remove_custom_metadata(Auth#auth.session_id, FileRef, json);

delete(#op_req{auth = Auth, data = Data, gri = #gri{id = FileGuid, aspect = rdf_metadata}}) ->
    FileRef = ?FILE_REF(FileGuid, maps:get(<<"resolve_symlink">>, Data, true)),
    mi_file_metadata:remove_custom_metadata(Auth#auth.session_id, FileRef, rdf).