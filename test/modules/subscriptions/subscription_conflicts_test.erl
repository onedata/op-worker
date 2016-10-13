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
                rev = <<"r4">>,
                value = Space
            },

            % when
            subscription_conflicts:update_model(od_space, UpdateDoc,
                [<<"r4">>, <<"r3">>, <<"r2">>, <<"r1">>]),

            %then
            {CreateDoc, _} = last_create_or_update(od_space),
            Expectation = UpdateDoc#document{
                key = <<"some key">>,
                value = Space#od_space{
                    revision_history = [<<"r4">>, <<"r3">>, <<"r2">>, <<"r1">>]
                }},
            ?assertMatch(Expectation, CreateDoc)
        end]}.

creates_docs_with_proper_rev_number_test_() ->
    {setup,
        fun setup/0,
        fun teardown/1,
        [fun() ->
            % given
            UpdateDoc = #document{
                rev = <<"rev_not_connected_with_update">>,
                value = #od_space{}
            },

            % when
            subscription_conflicts:update_model(od_space, UpdateDoc,
                [<<"r4">>, <<"r3">>, <<"r2">>, <<"r1">>]),

            %then
            {CreateDoc, _} = last_create_or_update(od_space),
            ?assertEqual(<<"r4">>, CreateDoc#document.rev)
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
                [<<"r4">>, <<"r3">>, <<"r2">>, <<"r1">>]),
            {_, UpdateFun} = last_create_or_update(od_space),
            CurrentRecord = #od_space{
                name = <<"space1">>,
                revision_history = [<<"r3">>, <<"r2">>, <<"r1">>]
            },

            % when
            Result = UpdateFun(CurrentRecord),

            %then
            ?assertMatch({ok, #od_space{
                name = <<"space1-up">>,
                revision_history = [<<"r4">>, <<"r3">>, <<"r2">>, <<"r1">>]
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
                [<<"r4">>, <<"r3">>, <<"r2">>, <<"r1">>]),
            {_, UpdateFun} = last_create_or_update(od_space),
            CurrentRecord = #od_space{
                name = <<"space1">>,
                revision_history = [<<"r3">>]
            },

            % when
            Result = UpdateFun(CurrentRecord),

            %then
            ?assertMatch({ok, #od_space{
                name = <<"space1-up">>,
                revision_history = [<<"r4">>, <<"r2">>, <<"r1">>, <<"r3">>]
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
                [<<"r2">>, <<"r1">>]),
            {_, UpdateFun} = last_create_or_update(od_space),
            CurrentRecord = #od_space{
                name = <<"space1">>,
                revision_history = [<<"r3">>, <<"r2">>, <<"r1">>]
            },

            % when
            Result = UpdateFun(CurrentRecord),

            %then
            ?assertMatch({ok, #od_space{
                name = <<"space1">>,
                revision_history = [<<"r3">>, <<"r2">>, <<"r1">>]
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
                [<<"r6">>, <<"r5">>, <<"r1">>]),
            {_, UpdateFun} = last_create_or_update(od_space),
            CurrentRecord = #od_space{
                name = <<"space1">>,
                revision_history = [<<"r6">>, <<"r4">>, <<"r3">>]
            },

            % when
            Result = UpdateFun(CurrentRecord),

            %then
            ?assertMatch({ok, #od_space{
                name = <<"space1">>,
                revision_history = [<<"r6">>, <<"r4">>, <<"r3">>, <<"r5">>, <<"r1">>]
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