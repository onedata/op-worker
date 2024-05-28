%%%-------------------------------------------------------------------
%%% @author Michał Stanisz
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
% fixme
%%% @end
%%%-------------------------------------------------------------------
-module(times_cache). % fixme name % fixme move
-author("Michal Stanisz").

-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").


%% API
-export([report_created/3, update/2, get/1, report_deleted/1]).

% fixme implement
%%%===================================================================
%%% API
%%%===================================================================

report_created(FileGuid, IgnoreInChanges, Times) ->
    % fixme explain not caching
    over_times:create(FileGuid, IgnoreInChanges, Times).
    
update(FileGuid, NewTimes) ->
    over_times:update(FileGuid, NewTimes).

get(FileGuid) ->
    over_times:get(FileGuid).

report_deleted(FileGuid) ->
    % fixme explain not caching
    over_times:delete(FileGuid).
