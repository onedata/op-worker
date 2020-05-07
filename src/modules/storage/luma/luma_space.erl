%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This is a helper module for luma_spaces_cache module.
%%% It encapsulates #posix_credentials{} record.
%%% @end
%%%-------------------------------------------------------------------
-module(luma_space).
-author("Jakub Kudzia").

%% API
-export([new/4, get_default_uid/1, get_default_gid/1, get_display_uid/1, get_display_gid/1]).

-record(posix_credentials, {
     % TODO opisać
    default_uid :: undefined | non_neg_integer(),
    default_gid :: undefined | non_neg_integer(),
    % todo opisać
    display_uid :: undefined | non_neg_integer(),
    display_gid :: undefined | non_neg_integer()
}).

-type posix_credentials() ::  #posix_credentials{}.
-export_type([posix_credentials/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec new(luma:uid(), luma:gid(), luma:uid(), luma:gid()) -> posix_credentials().
new(DefaultUid, DefaultGid, DisplayUid, DisplayGid) ->
    #posix_credentials{
        default_uid = DefaultUid,
        default_gid = DefaultGid,
        display_uid = DisplayUid,
        display_gid = DisplayGid
    }.

-spec get_default_uid(posix_credentials()) -> luma:uid().
get_default_uid(#posix_credentials{default_uid = DefaultUid}) ->
    DefaultUid.

-spec get_default_gid(posix_credentials()) -> luma:gid().
get_default_gid(#posix_credentials{default_gid = DefaultGid}) ->
    DefaultGid.

-spec get_display_uid(posix_credentials()) -> luma:uid().
get_display_uid(#posix_credentials{display_uid = DisplayUid}) ->
    DisplayUid.

-spec get_display_gid(posix_credentials()) -> luma:gid().
get_display_gid(#posix_credentials{display_gid = DisplayGid}) ->
    DisplayGid.