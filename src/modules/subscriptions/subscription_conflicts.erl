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
-export([update_model/3, delete_model/2]).

%%--------------------------------------------------------------------
%% @doc
%% Checks revision history and updates it with revisions from update.
%% Model is updated only if revisions check succeeds.
%% @end
%%--------------------------------------------------------------------
-spec update_model(Model :: subscriptions:model(),
    Update :: datastore:document(),
    UpdateRevs :: [subscriptions:rev()]) -> ok.
update_model(Model, UpdateDoc, UpdateRevs) ->
    try
        Key = UpdateDoc#document.key,
        UpdateRevs2 = [hd(lists:reverse(lists:sort(UpdateRevs)))],
        Update = UpdateDoc#document{
            value = set_revisions(Model, UpdateDoc#document.value, UpdateRevs2)
        },

        {ok, Key} = Model:create_or_update(Update, fun(Record) ->
            RevisionHistory = get_revisions(Model, Record),
            UpdatedRecord = case should_update(RevisionHistory, UpdateRevs2) of
                {false, UpdatedHistory} ->
                    set_revisions(Model, Record, UpdatedHistory);
                {true, UpdatedHistory} ->
                    NewRecord = Update#document.value,
                    set_revisions(Model, NewRecord, UpdatedHistory)
            end,
            {ok, UpdatedRecord}
        end)
    catch Error:Reason ->
        ?warning_stacktrace("Cannot apply changes from subscriptions - ~p:~p~",
            [Error, Reason])
    end,
    ok.

%%--------------------------------------------------------------------
%% @doc
%% As model isn't deleted then recreated in OZ (there is no such case)
%% the model is deleted without revision checks.
%% @end
%%--------------------------------------------------------------------
-spec delete_model(Model :: subscriptions:model(), ID :: datastore:ext_key()) ->
    ok.
delete_model(Model, ID) ->
    Model:delete(ID),
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc @private
%% Checks if an update should be applied by verification if revisions
%% connected with that update comply with current revisions history.
%% Returns new revision history including previously unseen revisions.
%% History is trimmed to prevent unchecked growth.
%% @end
%%--------------------------------------------------------------------
-spec should_update(HistoryRevs :: [subscriptions:rev()],
    UpdateRevs :: [subscriptions:rev()]) ->
    {boolean(), UpdatedHistoryRevs :: [subscriptions:rev()]}.
should_update([], UpdateRevs) ->
    {true, UpdateRevs};
should_update(HistoryRevs, UpdateRevs) ->
    UpdateCurrent = hd(UpdateRevs),
    UpdateLast = hd(HistoryRevs),
    case get_generation(UpdateCurrent) > get_generation(UpdateLast) of
        true -> {true, [UpdateCurrent]};
        false -> {false, [UpdateLast]}
    end.

%%--------------------------------------------------------------------
%% @doc @private
%% Extracts revisions history from document being synced with the OZ.
%% Such documents maintain revision history explicitly.
%% @end
%%--------------------------------------------------------------------
-spec get_revisions(Model :: atom(), Record :: subscriptions:record()) ->
    Revs :: [subscriptions:rev()].
get_revisions(od_space, Record) ->
    Record#od_space.revision_history;
get_revisions(od_share, Record) ->
    Record#od_share.revision_history;
get_revisions(od_user, Record) ->
    Record#od_user.revision_history;
get_revisions(od_group, Record) ->
    Record#od_group.revision_history;
get_revisions(od_provider, Record) ->
    Record#od_provider.revision_history;
get_revisions(od_handle, Record) ->
    Record#od_handle.revision_history;
get_revisions(od_handle_service, Record) ->
    Record#od_handle_service.revision_history.

%%--------------------------------------------------------------------
%% @doc @private
%% Sets revisions history to document being synced with the OZ.
%% Such documents maintain revision history explicitly.
%% @end
%%--------------------------------------------------------------------
-spec set_revisions(Model :: atom(), Record :: subscriptions:record(),
    Revs :: [subscriptions:rev()]) -> Record2 :: subscriptions:record().
set_revisions(od_space, Record, Revisions) ->
    Record#od_space{revision_history = Revisions};
set_revisions(od_share, Record, Revisions) ->
    Record#od_share{revision_history = Revisions};
set_revisions(od_user, Record = #od_user{public_only = true}, _) ->
    %% records with public data only do not have revisions
    %% thus always are overridden by "full" records
    Record#od_user{revision_history = []};
set_revisions(od_user, Record, Revisions) ->
    Record#od_user{revision_history = Revisions};
set_revisions(od_group, Record, Revisions) ->
    Record#od_group{revision_history = Revisions};
set_revisions(od_provider, Record, Revisions) ->
    Record#od_provider{revision_history = Revisions};
set_revisions(od_handle, Record, Revisions) ->
    Record#od_handle{revision_history = Revisions};
set_revisions(od_handle_service, Record, Revisions) ->
    Record#od_handle_service{revision_history = Revisions}.

%%--------------------------------------------------------------------
%% @doc @private
%% Returns revision generation.
%% @end
%%--------------------------------------------------------------------
-spec get_generation(Rev :: binary()) -> non_neg_integer().
get_generation(Rev) ->
    [Gen, _] = binary:split(Rev, <<"-">>, [global]),
    binary_to_integer(Gen).