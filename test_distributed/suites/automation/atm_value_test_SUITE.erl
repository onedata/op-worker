%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of automation values (instantiations of 'atm_data_type').
%%% @end
%%%-------------------------------------------------------------------
-module(atm_value_test_SUITE).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").
-include("onenv_test_utils.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

%% exported for CT
-export([
    groups/0, all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_group/2, end_per_group/2,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    atm_array_value_validation_test/1,
    atm_dataset_value_validation_test/1,
    atm_file_value_validation_test/1,
    atm_integer_value_validation_test/1,
    atm_object_value_validation_test/1,
    atm_onedatafs_credentials_value_validation_test/1,
    atm_range_value_validation_test/1,
    atm_string_value_validation_test/1,
    atm_time_series_measurement_value_validation_test/1
]).

groups() -> [
    {all_tests, [parallel], [
        atm_array_value_validation_test,
        atm_dataset_value_validation_test,
        atm_file_value_validation_test,
        atm_integer_value_validation_test,
        atm_object_value_validation_test,
        atm_onedatafs_credentials_value_validation_test,
        atm_range_value_validation_test,
        atm_string_value_validation_test,
        atm_time_series_measurement_value_validation_test
    ]}
].

all() -> [
    {group, all_tests}
].


-define(PROVIDER_SELECTOR, krakow).
-define(rpc(Expr), ?rpc(?PROVIDER_SELECTOR, Expr)).


%%%===================================================================
%%% API functions
%%%===================================================================


atm_array_value_validation_test(_Config) ->
    validate_value_test_base(
        #atm_data_spec{
            type = atm_array_type,
            value_constraints = #{item_data_spec => #atm_data_spec{
                type = atm_array_type,
                value_constraints = #{item_data_spec => #atm_data_spec{type = atm_integer_type}}
            }}
        },

        [
            [],
            [[]],
            [[1, 2, 3], [4, 5]],
            [[1, 2, 3], [], [4, 5]]
        ],

        lists:flatten([
            lists:map(fun(Value) ->
                {Value, ?ERROR_ATM_DATA_TYPE_UNVERIFIED(Value, atm_array_type)} end,
                [5.5, <<"NaN">>, #{<<"key">> => 5}]
            ),

            lists:map(fun({Value, ConstraintUnverified}) ->
                {Value, ?ERROR_ATM_DATA_VALUE_CONSTRAINT_UNVERIFIED(
                    Value, atm_array_type, ConstraintUnverified
                )}
            end, [
                {[[1, 2, 3], <<"NaN">>], #{<<"$[1]">> => errors:to_json(
                    ?ERROR_ATM_DATA_TYPE_UNVERIFIED(<<"NaN">>, atm_array_type)
                )}},
                {[[1, 2, 3, <<"NaN">>], [4, 5]], #{<<"$[0]">> => errors:to_json(
                    ?ERROR_ATM_DATA_VALUE_CONSTRAINT_UNVERIFIED([1, 2, 3, <<"NaN">>], atm_array_type, #{
                        <<"$[3]">> => errors:to_json(
                            ?ERROR_ATM_DATA_TYPE_UNVERIFIED(<<"NaN">>, atm_integer_type)
                        )
                    })
                )}}
            ])
        ])
    ).


atm_dataset_value_validation_test(_Config) ->
    SpaceKrkId = oct_background:get_space_id(space_krk),

    [
        #object{dataset = #dataset_object{id = FileInSpace1DatasetId}}
    ] = onenv_file_test_utils:create_and_sync_file_tree(
        user1, space1, [#file_spec{dataset = #dataset_spec{}}]
    ),

    [
        #object{dataset = #dataset_object{id = DirDatasetId}, children = [
            #object{dataset = #dataset_object{id = FileInDirDatasetId}}
        ]},
        #object{dataset = #dataset_object{id = FileDatasetId}},
        #object{dataset = #dataset_object{id = SymlinkDatasetId}}
    ] = onenv_file_test_utils:create_and_sync_file_tree(user1, space_krk, [
        #dir_spec{mode = 8#700, dataset = #dataset_spec{}, children = [
            #file_spec{dataset = #dataset_spec{}}
        ]},
        #file_spec{dataset = #dataset_spec{}},
        #symlink_spec{symlink_value = <<"a/b">>, dataset = #dataset_spec{}}
    ]),

    validate_value_test_base(
        #atm_data_spec{type = atm_dataset_type},

        lists:map(fun(DatasetId) -> #{<<"datasetId">> => DatasetId} end, [
            DirDatasetId,
            % user can view dataset even if he does not have access to file the dataset is attached
            FileInDirDatasetId,
            FileDatasetId,
            SymlinkDatasetId
        ]),

        lists:flatten([
            lists:map(fun(Value) ->
                {Value, ?ERROR_ATM_DATA_TYPE_UNVERIFIED(Value, atm_dataset_type)} end,
                [5.5, <<"NaN">>, [5], #{<<"datasetId">> => 5}]
            ),

            lists:map(fun({DatasetId, UnverifiedConstraint}) ->
                Value = #{<<"datasetId">> => DatasetId},
                {Value, ?ERROR_ATM_DATA_VALUE_CONSTRAINT_UNVERIFIED(
                    Value, atm_dataset_type, UnverifiedConstraint
                )}
            end, [
                % atm workflow execution is run in space_krk so only datasets
                % from that space can be processed
                {FileInSpace1DatasetId, #{<<"inSpace">> => SpaceKrkId}},

                % no access due to file not existing
                {<<"NonExistentDatasetId">>, #{<<"hasAccess">> => true}}
            ])
        ])
    ).


atm_file_value_validation_test(_Config) ->
    SpaceKrkId = oct_background:get_space_id(space_krk),

    [#object{guid = FileInSpace1Guid}] = onenv_file_test_utils:create_and_sync_file_tree(
        user1, space1, [#file_spec{}]
    ),
    [
        #object{guid = DirGuid, children = [#object{guid = FileInDirGuid}]},
        #object{guid = FileGuid},
        #object{guid = SymlinkGuid}
    ] = onenv_file_test_utils:create_and_sync_file_tree(user1, space_krk, [
        #dir_spec{mode = 8#700, children = [#file_spec{}]},
        #file_spec{},
        #symlink_spec{symlink_value = <<"a/b">>}
    ]),

    ValueConstraints = ?RAND_ELEMENT([
        #{},
        #{file_type => 'ANY'},
        #{file_type => 'REG'},
        #{file_type => 'DIR'},
        #{file_type => 'SYMLNK'}
    ]),
    AllowedFileType = maps:get(file_type, ValueConstraints, 'ANY'),
    AllowedFileTypeBin = str_utils:to_binary(AllowedFileType),

    {FilesWithAllowedType, FilesWithNotAllowedType} = case AllowedFileType of
        'ANY' -> {[DirGuid, FileGuid, SymlinkGuid], []};
        'REG' -> {[FileGuid], [DirGuid, SymlinkGuid]};
        'DIR' -> {[DirGuid], [FileGuid, SymlinkGuid]};
        'SYMLNK' -> {[SymlinkGuid], [DirGuid, FileGuid]}
    end,

    BuildFileObjectFun = fun(Guid) ->
        {ok, ObjectId} = file_id:guid_to_objectid(Guid),
        #{<<"file_id">> => ObjectId}
    end,

    validate_value_test_base(
        #atm_data_spec{type = atm_file_type, value_constraints = ValueConstraints},

        lists:map(fun(Guid) -> BuildFileObjectFun(Guid) end, FilesWithAllowedType),

        lists:flatten([
            lists:map(fun(Value) ->
                {Value, ?ERROR_ATM_DATA_TYPE_UNVERIFIED(Value, atm_file_type)} end,
                [5.5, <<"NaN">>, [5], #{<<"file_id">> => 5}]
            ),

            lists:map(fun({Guid, UnverifiedConstraint}) ->
                Value = BuildFileObjectFun(Guid),
                {Value, ?ERROR_ATM_DATA_VALUE_CONSTRAINT_UNVERIFIED(
                    Value, atm_file_type, UnverifiedConstraint
                )}
            end, [
                % atm workflow execution is run in space_krk so only files
                % from that space can be processed
                {FileInSpace1Guid, #{<<"inSpace">> => SpaceKrkId}},

                % no access due to file not existing
                {file_id:pack_guid(<<"NonExistentUuid">>, SpaceKrkId), #{<<"hasAccess">> => true}},

                % no access due to ancestor dir perms
                {FileInDirGuid, #{<<"hasAccess">> => true}}
            ]),

            lists:map(fun(Guid) ->
                Value = BuildFileObjectFun(Guid),
                {Value, ?ERROR_ATM_DATA_VALUE_CONSTRAINT_UNVERIFIED(
                    Value, atm_file_type, #{<<"fileType">> => AllowedFileTypeBin}
                )}
            end, FilesWithNotAllowedType)
        ])
    ).


atm_integer_value_validation_test(_Config) ->
    validate_value_test_base(
        #atm_data_spec{type = atm_integer_type},
        [-10, 0, 10],
        lists:map(fun(Value) ->
            {Value, ?ERROR_ATM_DATA_TYPE_UNVERIFIED(Value, atm_integer_type)} end,
            [5.5, [5], #{<<"key">> => 5}]
        )
    ).


atm_object_value_validation_test(_Config) ->
    validate_value_test_base(
        #atm_data_spec{type = atm_object_type},
        [
            #{<<"key1">> => <<"value">>},
            #{<<"key2">> => 5},
            #{<<"key3">> => [5]},
            #{<<"key4">> => #{<<"key">> => <<"value">>}},

            % atm values are encoded and decoded on system boundaries and as such
            % "improper" json terms (objects with non-binary keys) should never
            % appear in the system ("implicit" validation). Because of that
            % lower layers (tested here) will not check it and would accept
            % any valid map
            #{5 => 6}
        ],
        lists:map(fun(Value) ->
            {Value, ?ERROR_ATM_DATA_TYPE_UNVERIFIED(Value, atm_object_type)} end,
            [5.5, <<"NaN">>, [5]]
        )
    ).


atm_onedatafs_credentials_value_validation_test(_Config) ->
    validate_value_test_base(
        #atm_data_spec{type = atm_onedatafs_credentials_type},
        [
            % atm_onedatafs_credentials_value can be specified only as an argument
            % to lambda (they will be generated by op right before lambda invocation).
            % They can not appear in any other context and as such fields content
            % is not validated beside basic type checks.
            #{<<"host">> => <<"host">>, <<"accessToken">> => <<"token">>}
        ],
        lists:map(fun(Value) ->
            {Value, ?ERROR_ATM_DATA_TYPE_UNVERIFIED(Value, atm_onedatafs_credentials_type)} end,
            [
                5.5,
                <<"NaN">>,
                [5],
                #{<<"host">> => <<"host">>},
                #{<<"host">> => 5, <<"accessToken">> => <<"token">>}
                #{<<"host">> => <<"host">>, <<"accessToken">> => 5}
            ]
        )
    ).


atm_range_value_validation_test(_Config) ->
    validate_value_test_base(
        #atm_data_spec{type = atm_range_type},
        [
            #{<<"end">> => 10},
            #{<<"start">> => 1, <<"end">> => 10},
            #{<<"start">> => -5, <<"end">> => 10, <<"step">> => 2},
            #{<<"start">> => 15, <<"end">> => -10, <<"step">> => -1},

            % Valid objects with excess fields are also accepted
            #{<<"end">> => 100, <<"key">> => <<"value">>}
        ],
        lists:flatten([
            lists:map(
                fun(Value) -> {Value, ?ERROR_ATM_DATA_TYPE_UNVERIFIED(Value, atm_range_type)} end,
                [
                    5,
                    <<"NaN">>,
                    [5],
                    #{<<"key">> => 5},
                    #{<<"end">> => <<"NaN">>},
                    #{<<"start">> => <<"NaN">>, <<"end">> => 10},
                    #{<<"start">> => 5, <<"end">> => 10, <<"step">> => <<"NaN">>}
                ]
            ),

            lists:map(fun(Value) ->
                {Value, ?ERROR_ATM_DATA_VALUE_CONSTRAINT_UNVERIFIED(
                    Value, atm_range_type, <<"invalid step direction">>
                )}
            end, [
                #{<<"start">> => 5, <<"end">> => 10, <<"step">> => 0},
                #{<<"start">> => 15, <<"end">> => 10, <<"step">> => 1},
                #{<<"start">> => -15, <<"end">> => -10, <<"step">> => -1},
                #{<<"start">> => 10, <<"end">> => 15, <<"step">> => -1}
            ])
        ])
    ).


atm_string_value_validation_test(_Config) ->
    validate_value_test_base(
        #atm_data_spec{type = atm_string_type},
        [<<"">>, <<"NaN">>, <<"!@#$%^&*()">>],
        lists:map(fun(Value) ->
            {Value, ?ERROR_ATM_DATA_TYPE_UNVERIFIED(Value, atm_string_type)} end,
            [5, [5], #{<<"key">> => 5}]
        )
    ).


atm_time_series_measurement_value_validation_test(_Config) ->
    BuildTSMeasurement = fun(TSName) ->
        #{
            <<"tsName">> => TSName,
            <<"timestamp">> => ?RAND_INT(10000),
            <<"value">> => ?RAND_ELEMENT([1, -1]) * ?RAND_INT(10000)
        }
    end,

    MeasurementSpecs = [
        #atm_time_series_measurement_spec{
            name_matcher_type = exact,
            name_matcher = <<"size">>,
            unit = none
        },
        #atm_time_series_measurement_spec{
            name_matcher_type = has_prefix,
            name_matcher = <<"awesome_">>,
            unit = none
        }
    ],
    MeasurementSpecsJson = jsonable_record:list_to_json(
        MeasurementSpecs, atm_time_series_measurement_spec
    ),

    validate_value_test_base(
        #atm_data_spec{
            type = atm_time_series_measurement_type,
            value_constraints = #{specs => MeasurementSpecs}
        },
        [
            BuildTSMeasurement(<<"size">>),
            BuildTSMeasurement(<<"awesome_tests">>)
        ],
        lists:flatten([
            lists:map(fun(Value) ->
                {Value, ?ERROR_ATM_DATA_TYPE_UNVERIFIED(Value, atm_time_series_measurement_type)} end,
                [
                    <<"NaN">>,
                    5,
                    [5],
                    #{<<"key">> => 5},
                    #{<<"tsName">> => 5, <<"timestamp">> => 10, <<"value">> => 10},
                    #{<<"tsName">> => <<"size">>, <<"timestamp">> => <<"NaN">>, <<"value">> => -10},
                    #{<<"tsName">> => <<"size">>, <<"timestamp">> => 10, <<"value">> => <<"NaN">>},
                    #{<<"tsName">> => <<"size">>, <<"timestamp">> => -10, <<"value">> => 10}
                ]
            ),

            lists:map(fun(Value) ->
                {Value, ?ERROR_ATM_DATA_VALUE_CONSTRAINT_UNVERIFIED(
                    Value, atm_time_series_measurement_type, #{<<"specs">> => MeasurementSpecsJson}
                )}
            end, [
                BuildTSMeasurement(<<"ezis">>),
                BuildTSMeasurement(<<"awe_ja">>),
                BuildTSMeasurement(<<"awesome">>)
            ])
        ])
    ).


%% @private
validate_value_test_base(AtmDataSpec, ValidValues, InvalidValuesAndExpErrors) ->
    AtmWorkflowExecutionAuth = create_workflow_execution_auth(),

    lists:foreach(fun(ValidValue) ->
        ?assertEqual(ok, ?rpc(atm_value:validate(
            AtmWorkflowExecutionAuth, ValidValue, AtmDataSpec
        )))
    end, ValidValues),

    lists:foreach(fun({InvalidValue, ExpError}) ->
        ?assertEqual(ExpError, ?rpc(catch atm_value:validate(
            AtmWorkflowExecutionAuth, InvalidValue, AtmDataSpec
        )))
    end, InvalidValuesAndExpErrors).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec create_workflow_execution_auth() -> atm_workflow_execution_auth:record().
create_workflow_execution_auth() ->
    atm_store_test_utils:create_workflow_execution_auth(
        ?PROVIDER_SELECTOR, user2, space_krk
    ).


%===================================================================
% SetUp and TearDown functions
%===================================================================


init_per_suite(Config) ->
    ModulesToLoad = [?MODULE, atm_store_test_utils],
    oct_background:init_per_suite([{?LOAD_MODULES, ModulesToLoad} | Config], #onenv_test_config{
        onenv_scenario = "1op",
        envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_group(all_tests, Config) ->
    time_test_utils:freeze_time(Config),
    Config.


end_per_group(all_tests, Config) ->
    time_test_utils:unfreeze_time(Config).


init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 5}),
    Config.


end_per_testcase(_Case, _Config) ->
    ok.
