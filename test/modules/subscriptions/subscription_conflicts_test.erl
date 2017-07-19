%%%--------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Unit tests for conflict resolution in subscriptions.
%%% @end
%%%--------------------------------------------------------------------
-module(subscription_conflicts_test).
-author("Michal Zmuda").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include("global_definitions.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").

-define(MOCKED_MODELS, [od_space, od_group, od_user]).

-define(REV1, <<"1-r">>).
-define(REV2, <<"2-r">>).
-define(REV3, <<"3-r">>).
-define(REV4, <<"4-r">>).
-define(REV5, <<"5-r">>).
-define(REV6, <<"6-r">>).

%%%===================================================================
%%% Test functions
%%%===================================================================

creates_docs_with_proper_value_test_() ->
    {setup,
        fun setup/0,
        fun teardown/1,
        [fun() ->
            % given
            Space = #od_space{
                name = <<"space1name">>,
                revision_history = undefined
            },
            UpdateDoc = #document{
                key = <<"some key">>,
                rev = ?REV4,
                value = Space
            },

            % when
            subscription_conflicts:update_model(od_space, UpdateDoc,
                [?REV4, ?REV3, ?REV2, ?REV1]),

            %then
            {CreateDoc, _} = last_create_or_update(od_space),
            Expectation = UpdateDoc#document{
                key = <<"some key">>,
                value = Space#od_space{
                    revision_history = [?REV4]
                }},
            ?assertMatch(Expectation, CreateDoc)
        end]}.

accepts_unseen_revs_test_() ->
    {setup,
        fun setup/0,
        fun teardown/1,
        [fun() ->
            % given
            UpdateDoc = #document{
                rev = <<"rev_not_connected_with_update">>,
                value = #od_space{name = <<"space1-up">>}
            },
            subscription_conflicts:update_model(od_space, UpdateDoc,
                [?REV4, ?REV3, ?REV2, ?REV1]),
            {_, UpdateFun} = last_create_or_update(od_space),
            CurrentRecord = #od_space{
                name = <<"space1">>,
                revision_history = [?REV3, ?REV2, ?REV1]
            },

            % when
            Result = UpdateFun(CurrentRecord),

            %then
            ?assertMatch({ok, #od_space{
                name = <<"space1-up">>,
                revision_history = [?REV4]
            }}, Result)
        end]}.

includes_all_unseen_revs_on_accept_test_() ->
    {setup,
        fun setup/0,
        fun teardown/1,
        [fun() ->
            % given
            UpdateDoc = #document{
                rev = <<"rev_not_connected_with_update">>,
                value = #od_space{name = <<"space1-up">>}
            },
            subscription_conflicts:update_model(od_space, UpdateDoc,
                [?REV4, ?REV3, ?REV2, ?REV1]),
            {_, UpdateFun} = last_create_or_update(od_space),
            CurrentRecord = #od_space{
                name = <<"space1">>,
                revision_history = [?REV3]
            },

            % when
            Result = UpdateFun(CurrentRecord),

            %then
            ?assertMatch({ok, #od_space{
                name = <<"space1-up">>,
                revision_history = [?REV4]
            }}, Result)
        end]}.

ignores_seen_revs_test_() ->
    {setup,
        fun setup/0,
        fun teardown/1,
        [fun() ->
            % given
            UpdateDoc = #document{
                rev = <<"rev_not_connected_with_update">>,
                value = #od_space{name = <<"space1-up">>}
            },
            subscription_conflicts:update_model(od_space, UpdateDoc,
                [?REV2, ?REV1]),
            {_, UpdateFun} = last_create_or_update(od_space),
            CurrentRecord = #od_space{
                name = <<"space1">>,
                revision_history = [?REV3, ?REV2, ?REV1]
            },

            % when
            Result = UpdateFun(CurrentRecord),

            %then
            ?assertMatch({ok, #od_space{
                name = <<"space1">>,
                revision_history = [?REV3]
            }}, Result)
        end]}.

includes_all_unseen_revs_on_ignore_test_() ->
    {setup,
        fun setup/0,
        fun teardown/1,
        [fun() ->
            % given
            UpdateDoc = #document{
                rev = <<"rev_not_connected_with_update">>,
                value = #od_space{name = <<"space1-up">>}
            },
            subscription_conflicts:update_model(od_space, UpdateDoc,
                [?REV6, ?REV5, ?REV1]),
            {_, UpdateFun} = last_create_or_update(od_space),
            CurrentRecord = #od_space{
                name = <<"space1">>,
                revision_history = [?REV6, ?REV4, ?REV3]
            },

            % when
            Result = UpdateFun(CurrentRecord),

            %then
            ?assertMatch({ok, #od_space{
                name = <<"space1">>,
                revision_history = [?REV6]
            }}, Result)
        end]}.

%%%-------------------------------------------------------------------

setup() ->
    meck:new(application, [unstick]),
    meck:expect(application, get_env, fun
        (_, subscriptions_history_lenght_limit) -> {ok, 100}
    end),
    lists:foreach(fun(Model) ->
        meck:new(Model, [unstick]),
        meck:expect(Model, create_or_update, fun(_Doc, _Fun) ->
            {ok, _Doc#document.key}
        end)
    end, ?MOCKED_MODELS).

teardown(_) ->
    lists:foreach(fun(Model) ->
        ?assert(meck:validate(Model))
    end, ?MOCKED_MODELS),
    meck:unload().

%%%-------------------------------------------------------------------

last_create_or_update(Model) ->
    Doc = meck:capture(last, Model, create_or_update, ['_', '_'], 1),
    Fun = meck:capture(last, Model, create_or_update, ['_', '_'], 2),
    {Doc, Fun}.

-endif.