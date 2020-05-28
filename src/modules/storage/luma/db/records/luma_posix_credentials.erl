%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% WRITEME
%%% This is a helper module for luma_spaces_defaults module.
%%% It encapsulates #luma_space_defaults{} record, which stores
%%% POSIX-compatible credentials for given space.
%%%
%%% This record has 4 fields:
%%%  * storage_uid - uid field in luma:storage_credentials() on POSIX-compatible
%%%                  storages for virtual SpaceOwner
%%%  * storage_gid - gid field in luma:storage_credentials() on POSIX-compatible
%%%                  storages for ALL users in given space
%%%  * display_uid - uid field in luma:display_credentials() for virtual SpaceOwner
%%%  * display_gid - gid field in luma:display_credentials() for ALL users in
%%%                  given space
%%%
%%% For more info please read the docs of luma.erl and
%%% luma_spaces_defaults.erl modules.
%%% @end
%%%-------------------------------------------------------------------
-module(luma_posix_credentials).
-author("Jakub Kudzia").

-behaviour(luma_db_record).

%% API
-export([new/1, new/2, get_uid/1, get_gid/1, all_fields_defined/1]).

%% luma_db_record callbacks
-export([to_json/1, from_json/1]).

-record(luma_posix_credentials, {
    uid :: uid(),
    gid :: gid()
}).

-type uid() :: non_neg_integer().
-type gid() :: non_neg_integer().

-type credentials() ::  #luma_posix_credentials{}.
%% @formatter:off
-type credentials_map() :: #{
    binary() => uid() | gid()
    %% <<"uid">> => uid(),
    %% <<"gid">> => gid()
}.
%% @formatter:on

-export_type([credentials/0, credentials_map/0, uid/0, gid/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec new(credentials_map()) -> credentials().
new(PosixCompatibleCredentials) ->
    from_json(PosixCompatibleCredentials).

-spec new(uid(), gid()) -> credentials().
new(Uid, Gid) ->
    #luma_posix_credentials{uid = Uid, gid = Gid}.

-spec get_uid(credentials()) -> uid().
get_uid(#luma_posix_credentials{uid = Uid}) ->
    Uid.

-spec get_gid(credentials()) -> gid().
get_gid(#luma_posix_credentials{gid = Gid}) ->
    Gid.

-spec all_fields_defined(credentials_map()) -> boolean().
all_fields_defined(#{<<"uid">> := _, <<"gid">> := _}) ->
    true;
all_fields_defined(_) ->
    false.

%%%===================================================================
%%% luma_db_record callbacks
%%%===================================================================

-spec to_json(credentials()) -> credentials_map().
to_json(#luma_posix_credentials{uid = Uid, gid = Gid}) ->
    #{
        <<"uid">> => utils:undefined_to_null(Uid),
        <<"gid">> => utils:undefined_to_null(Gid)
    }.

-spec from_json(credentials_map()) -> credentials().
from_json(Json) ->
    #luma_posix_credentials{
        uid = utils:null_to_undefined(maps:get(<<"uid">>, Json, undefined)),
        gid = utils:null_to_undefined(maps:get(<<"gid">>, Json, undefined))
    }.