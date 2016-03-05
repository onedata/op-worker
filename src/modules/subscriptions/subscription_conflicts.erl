%%%--------------------------------------------------------------------
%%% @author Michal Żmuda
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for maintaining revisions history and
%%% revisions field of all documents originating in OZ.
%%% All creates/updates of that documents should be executed by this module.
%%% @end
%%%--------------------------------------------------------------------
-module(subscription_conflicts).
-author("Michal Zmuda").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([update_model/2]).

%%--------------------------------------------------------------------
%% @doc
%% Checks revision history and updates it with revisions from update.
%% Model is updated only if revisions check succeeds.
%% @end
%%--------------------------------------------------------------------

-spec update_model(Model :: atom(), Update :: datastore:document())
        -> no_return().
update_model(Model, UpdateInput) ->
    UpdateRevs = get_revisions(Model, UpdateInput#document.value),
    Update = UpdateInput#document{rev = hd(UpdateRevs)},
    Key = Update#document.key,

    {ok, Key} = Model:create_or_update(Update, fun(Record) ->
        RevisionHistory = get_revisions(Model, Record),
        UpdatedRecord = case should_update(RevisionHistory, UpdateRevs) of
            {false, UpdatedHistory} ->
                set_revisions(Model, Record, UpdatedHistory);
            {true, UpdatedHistory} ->
                NewRecord = Update#document.value,
                set_revisions(Model, NewRecord, UpdatedHistory)
        end,
        {ok, UpdatedRecord}
    end).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @private
%% Checks if an update should be applied by verification if revisions
%% connected with that update comply with current revisions history.
%% Returns new revision history including previously unseen revisions.
%% @end
%%--------------------------------------------------------------------

-spec should_update(HistoryRevs :: [binary()], UpdateRevs :: [binary()])
        -> {boolean(), UpdatedHistoryRevs :: [binary()]}.

should_update(HistoryRevs, UpdateRevs) ->
    UpdateCurrent = hd(UpdateRevs),
    NewRevs = UpdateRevs -- HistoryRevs,
    case lists:member(UpdateCurrent, HistoryRevs) of
        true ->
            {false, HistoryRevs ++ NewRevs};
        false ->
            {true, NewRevs ++ HistoryRevs}
    end.
-spec get_revisions(Model :: atom(), Record) -> Record2 when
    Record :: #space_info{} | #onedata_user{} | #onedata_group{},
    Record2 :: #space_info{} | #onedata_user{} | #onedata_group{}.
get_revisions(space_info, Record) ->
    Record#space_info.revision_history;
get_revisions(onedata_user, Record) ->
    Record#onedata_user.revision_history;
get_revisions(onedata_group, Record) ->
    Record#onedata_group.revision_history;
get_revisions(_Model, _Record) ->
    erlang:error(not_implemented).

set_revisions(space_info, Record, Revisions) ->
    Record#space_info{revision_history = Revisions};
set_revisions(onedata_user, Record, Revisions) ->
    Record#onedata_user{revision_history = Revisions};
set_revisions(onedata_group, Record, Revisions) ->
    Record#onedata_group{revision_history = Revisions};
set_revisions(_Model, _Record, _Revisions) ->
    erlang:error(not_implemented).