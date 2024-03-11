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

-include("atm/atm_test_schema_drafts.hrl").
-include("modules/automation/atm_execution.hrl").
-include("modules/logical_file_manager/lfm.hrl").
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

% tests
-export([
    atm_array_value_validation_test/1,
    atm_array_value_to_from_store_item_test/1,
    atm_array_value_describe_test/1,

    atm_boolean_value_validation_test/1,
    atm_boolean_value_to_from_store_item_test/1,
    atm_boolean_value_describe_test/1,

    atm_dataset_value_validation_test/1,
    atm_dataset_value_to_from_store_item_test/1,
    atm_dataset_value_describe_test/1,

    atm_file_value_validation_test/1,
    atm_file_value_to_from_store_item_test/1,
    atm_file_value_describe_test/1,

    atm_number_value_validation_test/1,
    atm_number_value_to_from_store_item_test/1,
    atm_number_value_describe_test/1,

    atm_object_value_validation_test/1,
    atm_object_value_to_from_store_item_test/1,
    atm_object_value_describe_test/1,

    atm_range_value_validation_test/1,
    atm_range_value_to_from_store_item_test/1,
    atm_range_value_describe_test/1,

    atm_string_value_validation_test/1,
    atm_string_value_to_from_store_item_test/1,
    atm_string_value_describe_test/1,

    atm_time_series_measurement_value_validation_test/1,
    atm_time_series_measurement_value_to_from_store_item_test/1,
    atm_time_series_measurement_value_describe_test/1
]).

groups() -> [
    {all_tests, [parallel], [
        atm_array_value_validation_test,
        atm_array_value_to_from_store_item_test,
        atm_array_value_describe_test,

        atm_boolean_value_validation_test,
        atm_boolean_value_to_from_store_item_test,
        atm_boolean_value_describe_test,

        atm_dataset_value_validation_test,
        atm_dataset_value_to_from_store_item_test,
        atm_dataset_value_describe_test,

        atm_file_value_validation_test,
        atm_file_value_to_from_store_item_test,
        atm_file_value_describe_test,

        atm_number_value_validation_test,
        atm_number_value_to_from_store_item_test,
        atm_number_value_describe_test,

        atm_object_value_validation_test,
        atm_object_value_to_from_store_item_test,
        atm_object_value_describe_test,

        atm_range_value_validation_test,
        atm_range_value_to_from_store_item_test,
        atm_range_value_describe_test,

        atm_string_value_validation_test,
        atm_string_value_to_from_store_item_test,
        atm_string_value_describe_test,

        atm_time_series_measurement_value_validation_test,
        atm_time_series_measurement_value_to_from_store_item_test,
        atm_time_series_measurement_value_describe_test
    ]}
].

all() -> [
    {group, all_tests}
].


-record(atm_value_validation_testcase, {
    data_spec,
    valid_values,
    invalid_values_with_exp_errors
}).
-record(atm_value_to_from_store_item_testcase, {
    data_spec,
    values :: [
        Term :: json_utils:json_term() |
        {different, Input :: json_utils:json_term(), ExpOutput :: json_utils:json_term()} |
        {error, Input :: json_utils:json_term(), ExpError :: errors:error()}
    ]
}).
-record(atm_value_describe_testcase, {
    data_spec,
    values :: [
        Term :: json_utils:json_term() |
        {different, Input :: json_utils:json_term(), ExpOutput :: json_utils:json_term()} |
        {error, Input :: json_utils:json_term(), ExpError :: errors:error()}
    ]
}).


-define(PROVIDER_SELECTOR, krakow).
-define(rpc(Expr), ?rpc(?PROVIDER_SELECTOR, Expr)).

-define(ok(__EXPR), element(2, {ok, _} = __EXPR)).


%%%===================================================================
%%% API functions
%%%===================================================================


atm_array_value_validation_test(_Config) ->
    atm_value_validation_test_base(#atm_value_validation_testcase{
        data_spec = #atm_array_data_spec{item_data_spec = #atm_array_data_spec{
            item_data_spec = #atm_number_data_spec{
                integers_only = false,
                allowed_values = undefined
            }
        }},
        valid_values = [
            [],
            [[]],
            [[1, 2, 3], [4, 5]],
            [[1, 2, 3], [], [4, 5]]
        ],
        invalid_values_with_exp_errors = lists:flatten([
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
                            ?ERROR_ATM_DATA_TYPE_UNVERIFIED(<<"NaN">>, atm_number_type)
                        )
                    })
                )}}
            ])
        ])
    }).


atm_array_value_to_from_store_item_test(_Config) ->
    atm_value_to_from_store_item_test_base(#atm_value_to_from_store_item_testcase{
        data_spec = #atm_array_data_spec{item_data_spec = #atm_array_data_spec{
            item_data_spec = #atm_range_data_spec{}
        }},
        values = [
            [],
            [[]],

            % Array items should be compressed and expanded according to their type rules
            {
                different,
                [
                    [],
                    [
                        #{<<"end">> => 100},
                        #{<<"end">> => 100, <<"start">> => -100},
                        #{<<"start">> => 15, <<"end">> => -10, <<"step">> => -1}
                    ]
                ],
                [
                    [],
                    [
                        #{<<"end">> => 100, <<"start">> => 0, <<"step">> => 1},
                        #{<<"end">> => 100, <<"start">> => -100, <<"step">> => 1},
                        #{<<"start">> => 15, <<"end">> => -10, <<"step">> => -1}
                    ]
                ]
            }
        ]
    }).


atm_array_value_describe_test(_Config) ->
    atm_value_describe_test_base(#atm_value_describe_testcase{
        data_spec = #atm_array_data_spec{item_data_spec = #atm_array_data_spec{
            item_data_spec = #atm_range_data_spec{}
        }},
        values = [
            [],
            [[]],

            % Array items should be described according to their type rules
            {
                different,
                [
                    [],
                    [
                        #{<<"end">> => 100},
                        #{<<"end">> => 100, <<"start">> => -100},
                        #{<<"start">> => 15, <<"end">> => -10, <<"step">> => -1}
                    ]
                ],
                [
                    [],
                    [
                        #{<<"end">> => 100, <<"start">> => 0, <<"step">> => 1},
                        #{<<"end">> => 100, <<"start">> => -100, <<"step">> => 1},
                        #{<<"start">> => 15, <<"end">> => -10, <<"step">> => -1}
                    ]
                ]
            }
        ]
    }).


atm_boolean_value_validation_test(_Config) ->
    atm_value_validation_test_base(#atm_value_validation_testcase{
        data_spec = #atm_boolean_data_spec{},
        valid_values = [false, true],
        invalid_values_with_exp_errors = lists:map(fun(Value) ->
            {Value, ?ERROR_ATM_DATA_TYPE_UNVERIFIED(Value, atm_boolean_type)} end,
            [5, <<"true">>, [5], #{<<"key">> => 5}]
        )
    }).


atm_boolean_value_to_from_store_item_test(_Config) ->
    atm_value_to_from_store_item_test_base(#atm_value_to_from_store_item_testcase{
        data_spec = #atm_boolean_data_spec{},
        values = [false, true]
    }).


atm_boolean_value_describe_test(_Config) ->
    atm_value_describe_test_base(#atm_value_describe_testcase{
        data_spec = #atm_boolean_data_spec{},
        values = [false, true]
    }).


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

    atm_value_validation_test_base(#atm_value_validation_testcase{
        data_spec = #atm_dataset_data_spec{},
        valid_values = lists:map(fun(DatasetId) -> #{<<"datasetId">> => DatasetId} end, [
            DirDatasetId,
            % user can view dataset even if he does not have access to file the dataset is attached
            FileInDirDatasetId,
            FileDatasetId,
            SymlinkDatasetId
        ]),
        invalid_values_with_exp_errors = lists:flatten([
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
    }).


atm_dataset_value_to_from_store_item_test(_Config) ->
    SessionId = oct_background:get_user_session_id(user1, krakow),

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

    atm_value_to_from_store_item_test_base(#atm_value_to_from_store_item_testcase{
        data_spec = #atm_dataset_data_spec{},
        values = lists:flatten([
            #{<<"datasetId">> => <<"RemovedDatasetId">>},

            lists:map(fun(DatasetId) ->
                DatasetInfo = ?rpc(mi_datasets:get_info(SessionId, DatasetId)),

                % atm_dataset_type is but a reference to underlying file entity -
                % as such expanding it should fetch all current file attributes
                {
                    different,
                    dataset_utils:dataset_info_to_json(DatasetInfo),
                    #{<<"datasetId">> => DatasetId}
                }
            end, [
                DirDatasetId,
                % user can view dataset even if he does not have access to file the dataset is attached
                FileInDirDatasetId,
                FileDatasetId,
                SymlinkDatasetId
            ])
        ])
    }).


atm_dataset_value_describe_test(_Config) ->
    SessionId = oct_background:get_user_session_id(user1, krakow),

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

    atm_value_describe_test_base(#atm_value_describe_testcase{
        data_spec = #atm_dataset_data_spec{},
        values = lists:flatten([
            {error, #{<<"datasetId">> => <<"RemovedDatasetId">>}, ?ERROR_NOT_FOUND},

            lists:map(fun(DatasetId) ->
                DatasetInfo = ?rpc(mi_datasets:get_info(SessionId, DatasetId)),

                % atm_dataset_type is but a reference to underlying file entity -
                % as such expanding it should fetch all current file attributes
                {
                    different,
                    #{<<"datasetId">> => DatasetId},
                    dataset_utils:dataset_info_to_json(DatasetInfo)
                }
            end, [
                DirDatasetId,
                % user can view dataset even if he does not have access to file the dataset is attached
                FileInDirDatasetId,
                FileDatasetId,
                SymlinkDatasetId
            ])
        ])
    }).


atm_file_value_validation_test(_Config) ->
    SpaceKrkId = oct_background:get_space_id(space_krk),
    SessionId = oct_background:get_user_session_id(user1, krakow),

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

    AllowedFileType = ?RAND_ELEMENT(['ANY', 'REG', 'DIR', 'SYMLNK']),
    AllowedFileTypeBin = str_utils:to_binary(AllowedFileType),

    {FilesWithAllowedType, FilesWithNotAllowedType} = case AllowedFileType of
        'ANY' -> {[DirGuid, FileGuid, SymlinkGuid], []};
        'REG' -> {[FileGuid], [DirGuid, SymlinkGuid]};
        'DIR' -> {[DirGuid], [FileGuid, SymlinkGuid]};
        'SYMLNK' -> {[SymlinkGuid], [DirGuid, FileGuid]}
    end,

    FileAttrsToResolve = ?RAND_SUBLIST(?API_FILE_ATTRS),

    BuildBareFileObjectFun = fun(Guid) ->
        {ok, ObjectId} = file_id:guid_to_objectid(Guid),
        #{<<"fileId">> => ObjectId}
    end,

    atm_value_validation_test_base(#atm_value_validation_testcase{
        data_spec = #atm_file_data_spec{
            file_type = AllowedFileType,
            attributes = FileAttrsToResolve
        },
        valid_values = lists:map(fun(Guid) ->
            BareFileObject = BuildBareFileObjectFun(Guid),
            {ok, FileAttrs} = ?rpc(lfm:stat(SessionId, ?FILE_REF(Guid), FileAttrsToResolve)),
            ResolvedFileObject = file_attr_translator:to_json(FileAttrs, current, FileAttrsToResolve),
            {BareFileObject, ResolvedFileObject}
        end, FilesWithAllowedType),
        invalid_values_with_exp_errors = lists:flatten([
            lists:map(fun(Value) ->
                {Value, ?ERROR_ATM_DATA_TYPE_UNVERIFIED(Value, atm_file_type)} end,
                [5.5, <<"NaN">>, [5], #{<<"fileId">> => 5}]
            ),

            lists:map(fun({Guid, UnverifiedConstraint}) ->
                Value = BuildBareFileObjectFun(Guid),
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
                Value = BuildBareFileObjectFun(Guid),
                {Value, ?ERROR_ATM_DATA_VALUE_CONSTRAINT_UNVERIFIED(
                    Value, atm_file_type, #{<<"fileType">> => AllowedFileTypeBin}
                )}
            end, FilesWithNotAllowedType)
        ])
    }).


atm_file_value_to_from_store_item_test(_Config) ->
    SpaceKrkId = oct_background:get_space_id(space_krk),
    SessionId = oct_background:get_user_session_id(user1, krakow),

    [
        #object{guid = DirGuid, children = [#object{guid = FileInDirGuid}]},
        #object{guid = FileGuid},
        #object{guid = SymlinkGuid}
    ] = onenv_file_test_utils:create_and_sync_file_tree(user1, space_krk, [
        #dir_spec{mode = 8#700, children = [#file_spec{}]},
        #file_spec{},
        #symlink_spec{symlink_value = <<"a/b">>}
    ]),

    atm_value_to_from_store_item_test_base(#atm_value_to_from_store_item_testcase{
        data_spec = #atm_file_data_spec{
            file_type = ?RAND_ELEMENT(['ANY', 'REG', 'DIR', 'SYMLNK']),
            % attributes shouldn't have any impact on compress/expand functionality
            attributes = ?RAND_SUBLIST(?ATM_FILE_ATTRIBUTES)
        },
        values = lists:flatten([
            #{<<"fileId">> => ?ok(file_id:guid_to_objectid(file_id:pack_guid(
                <<"removed_file_id">>, SpaceKrkId
            )))},
            #{<<"fileId">> => ?ok(file_id:guid_to_objectid(FileInDirGuid))},

            lists:map(fun(Guid) ->
                {ok, FileAttrs} = ?rpc(lfm:stat(SessionId, ?FILE_REF(Guid), ?ATM_FILE_VALUE_DESCRIBE_ATTRS)),

                % atm_file_type is but a reference to underlying file entity -
                % as such compressing and expanding should remove any attribute but file_id
                {
                    different,
                    file_attr_translator:to_json(FileAttrs, current, ?ATM_FILE_VALUE_DESCRIBE_ATTRS),
                    #{<<"fileId">> => ?ok(file_id:guid_to_objectid(Guid))}
                }
            end, [DirGuid, FileGuid, SymlinkGuid])
        ])
    }).


atm_file_value_describe_test(_Config) ->
    SpaceKrkId = oct_background:get_space_id(space_krk),
    SessionId = oct_background:get_user_session_id(user1, krakow),

    [
        #object{guid = DirGuid, children = [#object{guid = FileInDirGuid}]},
        #object{guid = FileGuid},
        #object{guid = SymlinkGuid}
    ] = onenv_file_test_utils:create_and_sync_file_tree(user1, space_krk, [
        #dir_spec{mode = 8#700, children = [#file_spec{}]},
        #file_spec{},
        #symlink_spec{symlink_value = <<"a/b">>}
    ]),

    atm_value_describe_test_base(#atm_value_describe_testcase{
        data_spec = #atm_file_data_spec{
            file_type = ?RAND_ELEMENT(['ANY', 'REG', 'DIR', 'SYMLNK']),
            % attributes shouldn't have any impact on describe functionality
            attributes = ?RAND_SUBLIST(?ATM_FILE_ATTRIBUTES)
        },
        values = lists:flatten([
            {
                error,
                #{<<"fileId">> => ?ok(file_id:guid_to_objectid(file_id:pack_guid(
                    <<"removed_file_id">>, SpaceKrkId
                )))},
                ?ERROR_POSIX(?ENOENT)
            },
            {
                error,
                #{<<"fileId">> => ?ok(file_id:guid_to_objectid(FileInDirGuid))},
                ?ERROR_POSIX(?EACCES)
            },

            lists:map(fun(Guid) ->
                {ok, FileAttrs} = ?rpc(lfm:stat(SessionId, ?FILE_REF(Guid))),

                % atm_file_type is but a reference to underlying file entity -
                % as such describe should resolve all attributes
                {
                    different,
                    #{<<"fileId">> => ?ok(file_id:guid_to_objectid(Guid))},
                    file_attr_translator:to_json(FileAttrs, current, ?ATM_FILE_VALUE_DESCRIBE_ATTRS)
                }
            end, [DirGuid, FileGuid, SymlinkGuid])
        ])
    }).


atm_number_value_validation_test(_Config) ->
    atm_value_validation_test_base(#atm_value_validation_testcase{
        data_spec = #atm_number_data_spec{
            integers_only = false,
            allowed_values = undefined
        },
        valid_values = [-10, 0, 5.5, 10],
        invalid_values_with_exp_errors = lists:map(fun(Value) ->
            {Value, ?ERROR_ATM_DATA_TYPE_UNVERIFIED(Value, atm_number_type)} end,
            [<<"5.5">>, [5], #{<<"key">> => 5}]
        )
    }),

    atm_value_validation_test_base(#atm_value_validation_testcase{
        data_spec = #atm_number_data_spec{
            integers_only = true,
            allowed_values = undefined
        },
        valid_values = [-10, 0, 10],
        invalid_values_with_exp_errors = [
            {5.5, ?ERROR_ATM_DATA_VALUE_CONSTRAINT_UNVERIFIED(
                5.5, atm_number_type, #{<<"integersOnly">> => true}
            )}
        ]
    }),

    AllowedValues = ?RAND_ELEMENT([[], [-4, 0, 5.6, 10]]),
    atm_value_validation_test_base(#atm_value_validation_testcase{
        data_spec = #atm_number_data_spec{
            integers_only = false,
            allowed_values = AllowedValues
        },
        valid_values = AllowedValues,
        invalid_values_with_exp_errors = lists:map(fun(Num) ->
            {Num, ?ERROR_ATM_DATA_VALUE_CONSTRAINT_UNVERIFIED(
                Num, atm_number_type, #{<<"allowedValues">> => AllowedValues}
            )}
        end, [-10, 0.1, 5.5, 7])
    }).


atm_number_value_to_from_store_item_test(_Config) ->
    atm_value_to_from_store_item_test_base(#atm_value_to_from_store_item_testcase{
        data_spec = #atm_number_data_spec{integers_only = false, allowed_values = undefined},
        values = [-10, 0, 5.5, 10]
    }).


atm_number_value_describe_test(_Config) ->
    atm_value_describe_test_base(#atm_value_describe_testcase{
        data_spec = #atm_number_data_spec{integers_only = false, allowed_values = undefined},
        values = [-10, 0, 5.5, 10]
    }).


atm_object_value_validation_test(_Config) ->
    atm_value_validation_test_base(#atm_value_validation_testcase{
        data_spec = #atm_object_data_spec{},
        valid_values = [
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
        invalid_values_with_exp_errors = lists:map(fun(Value) ->
            {Value, ?ERROR_ATM_DATA_TYPE_UNVERIFIED(Value, atm_object_type)} end,
            [5.5, <<"NaN">>, [5]]
        )
    }).


atm_object_value_to_from_store_item_test(_Config) ->
    atm_value_to_from_store_item_test_base(#atm_value_to_from_store_item_testcase{
        data_spec = #atm_object_data_spec{},
        values = [
            #{<<"key1">> => <<"value">>},
            #{<<"key2">> => 5},
            #{<<"key3">> => [5]},
            #{<<"key4">> => #{<<"key">> => <<"value">>}}
        ]
    }).


atm_object_value_describe_test(_Config) ->
    atm_value_describe_test_base(#atm_value_describe_testcase{
        data_spec = #atm_object_data_spec{},
        values = [
            #{<<"key1">> => <<"value">>},
            #{<<"key2">> => 5},
            #{<<"key3">> => [5]},
            #{<<"key4">> => #{<<"key">> => <<"value">>}}
        ]
    }).


atm_range_value_validation_test(_Config) ->
    atm_value_validation_test_base(#atm_value_validation_testcase{
        data_spec = #atm_range_data_spec{},
        valid_values = [
            #{<<"end">> => 10},
            #{<<"start">> => 1, <<"end">> => 10},
            #{<<"start">> => -5, <<"end">> => 10, <<"step">> => 2},
            #{<<"start">> => 15, <<"end">> => -10, <<"step">> => -1},

            % Valid objects with excess fields are also accepted
            #{<<"end">> => 100, <<"key">> => <<"value">>}
        ],
        invalid_values_with_exp_errors = lists:flatten([
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
    }).


atm_range_value_to_from_store_item_test(_Config) ->
    atm_value_to_from_store_item_test_base(#atm_value_to_from_store_item_testcase{
        data_spec = #atm_range_data_spec{},
        values = [
            #{<<"start">> => -5, <<"end">> => 10, <<"step">> => 2},
            #{<<"start">> => 15, <<"end">> => -10, <<"step">> => -1},

            % Optional fields should be filled with defaults when expanding
            % if not previously specified
            {
                different,
                #{<<"end">> => 10},
                #{<<"end">> => 10, <<"start">> => 0, <<"step">> => 1}
            },
            {
                different,
                #{<<"start">> => 1, <<"end">> => 10},
                #{<<"start">> => 1, <<"end">> => 10, <<"step">> => 1}
            },

            % Excess fields should be removed when compressing
            {
                different,
                #{<<"start">> => -5, <<"end">> => 10, <<"step">> => 2, <<"key">> => <<"value">>},
                #{<<"start">> => -5, <<"end">> => 10, <<"step">> => 2}
            }
        ]
    }).


atm_range_value_describe_test(_Config) ->
    atm_value_describe_test_base(#atm_value_describe_testcase{
        data_spec = #atm_range_data_spec{},
        values = [
            #{<<"start">> => -5, <<"end">> => 10, <<"step">> => 2},
            #{<<"start">> => 15, <<"end">> => -10, <<"step">> => -1},

            % Optional fields should be filled with defaults when expanding
            % if not previously specified
            {
                different,
                #{<<"end">> => 10},
                #{<<"end">> => 10, <<"start">> => 0, <<"step">> => 1}
            },
            {
                different,
                #{<<"start">> => 1, <<"end">> => 10},
                #{<<"start">> => 1, <<"end">> => 10, <<"step">> => 1}
            },

            % Excess fields should be removed when compressing
            {
                different,
                #{<<"start">> => -5, <<"end">> => 10, <<"step">> => 2, <<"key">> => <<"value">>},
                #{<<"start">> => -5, <<"end">> => 10, <<"step">> => 2}
            }
        ]
    }).


atm_string_value_validation_test(_Config) ->
    atm_value_validation_test_base(#atm_value_validation_testcase{
        data_spec = #atm_string_data_spec{allowed_values = undefined},
        valid_values = [<<"">>, <<"NaN">>, <<"!@#$%^&*()">>],
        invalid_values_with_exp_errors = lists:map(fun(Value) ->
            {Value, ?ERROR_ATM_DATA_TYPE_UNVERIFIED(Value, atm_string_type)} end,
            [5, [5], #{<<"key">> => 5}]
        )
    }),

    AllowedValues = ?RAND_ELEMENT([[], [<<"NaN">>, <<"!@#$%^&*()">>]]),
    atm_value_validation_test_base(#atm_value_validation_testcase{
        data_spec = #atm_string_data_spec{allowed_values = AllowedValues},
        valid_values = AllowedValues,
        invalid_values_with_exp_errors = lists:map(fun(Num) ->
            {Num, ?ERROR_ATM_DATA_VALUE_CONSTRAINT_UNVERIFIED(
                Num, atm_string_type, #{<<"allowedValues">> => AllowedValues}
            )}
        end, [<<"-10">>, <<"">>, <<"asd">>])
    }).


atm_string_value_to_from_store_item_test(_Config) ->
    atm_value_to_from_store_item_test_base(#atm_value_to_from_store_item_testcase{
        data_spec = #atm_string_data_spec{allowed_values = undefined},
        values = [<<"">>, <<"NaN">>, <<"!@#$%^&*()">>]
    }).


atm_string_value_describe_test(_Config) ->
    atm_value_describe_test_base(#atm_value_describe_testcase{
        data_spec = #atm_string_data_spec{allowed_values = undefined},
        values = [<<"">>, <<"NaN">>, <<"!@#$%^&*()">>]
    }).


atm_time_series_measurement_value_validation_test(_Config) ->
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

    atm_value_validation_test_base(#atm_value_validation_testcase{
        data_spec = #atm_time_series_measurement_data_spec{specs = MeasurementSpecs},
        valid_values = [
            build_rand_ts_measurement(<<"size">>),
            build_rand_ts_measurement(<<"awesome_tests">>)
        ],
        invalid_values_with_exp_errors = lists:flatten([
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
                    Value, atm_time_series_measurement_type, #{
                        <<"specs">> => jsonable_record:list_to_json(
                            MeasurementSpecs, atm_time_series_measurement_spec
                        )
                    }
                )}
            end, [
                build_rand_ts_measurement(<<"ezis">>),
                build_rand_ts_measurement(<<"awe_ja">>),
                build_rand_ts_measurement(<<"awesome">>)
            ])
        ])
    }).


atm_time_series_measurement_value_to_from_store_item_test(_Config) ->
    RandMeasurement = build_rand_ts_measurement(),

    atm_value_to_from_store_item_test_base(#atm_value_to_from_store_item_testcase{
        data_spec = #atm_time_series_measurement_data_spec{
            specs = [#atm_time_series_measurement_spec{
                name_matcher_type = has_prefix,
                name_matcher = <<"awesome_">>,
                unit = none
            }]
        },
        values = [
            % Compress and expand should work even for measurements not conforming
            % to value constraints as those are not checked for these operations
            build_rand_ts_measurement(),
            build_rand_ts_measurement(),
            build_rand_ts_measurement(),
            build_rand_ts_measurement(),
            build_rand_ts_measurement(),

            % Excess fields should be removed when compressing
            {different, RandMeasurement#{<<"key">> => <<"value">>}, RandMeasurement}
        ]
    }).


atm_time_series_measurement_value_describe_test(_Config) ->
    atm_value_describe_test_base(#atm_value_describe_testcase{
        data_spec = #atm_time_series_measurement_data_spec{
            specs = [#atm_time_series_measurement_spec{
                name_matcher_type = has_prefix,
                name_matcher = <<"awesome_">>,
                unit = none
            }]
        },
        values = [
            % Describe should work even for measurements not conforming
            % to value constraints as those are not checked for these operations
            build_rand_ts_measurement(),
            build_rand_ts_measurement(),
            build_rand_ts_measurement(),
            build_rand_ts_measurement(),
            build_rand_ts_measurement()
        ]
    }).


%% @private
atm_value_validation_test_base(#atm_value_validation_testcase{
    data_spec = AtmDataSpec,
    valid_values = ValidValues,
    invalid_values_with_exp_errors = InvalidValuesAndExpErrors
}) ->
    AtmWorkflowExecutionAuth = create_workflow_execution_auth(),

    lists:foreach(fun
        ({ValidValue, ValidResolvedValue}) ->
            ?assertEqual(ok, ?rpc(atm_value:validate_constraints(
                AtmWorkflowExecutionAuth, ValidValue, AtmDataSpec
            ))),
            ?assertEqual(ValidResolvedValue, ?rpc(atm_value:transform_to_data_spec_conformant(
                AtmWorkflowExecutionAuth, ValidValue, AtmDataSpec
            )));
        (ValidValue) ->
            ?assertEqual(ok, ?rpc(atm_value:validate_constraints(
                AtmWorkflowExecutionAuth, ValidValue, AtmDataSpec
            )))
    end, ValidValues),

    lists:foreach(fun({InvalidValue, ExpError}) ->
        ?assertEqual(ExpError, ?rpc(catch atm_value:validate_constraints(
            AtmWorkflowExecutionAuth, InvalidValue, AtmDataSpec
        ))),
        ?assertEqual(ExpError, ?rpc(catch atm_value:transform_to_data_spec_conformant(
            AtmWorkflowExecutionAuth, InvalidValue, AtmDataSpec
        )))
    end, InvalidValuesAndExpErrors).


%% @private
atm_value_to_from_store_item_test_base(#atm_value_to_from_store_item_testcase{
    data_spec = AtmDataSpec,
    values = Values
}) ->
    AtmWorkflowExecutionAuth = create_workflow_execution_auth(),

    lists:foreach(fun(Value) ->
        {InitialItem, ExpectedExpandResult} = case Value of
            {error, Input, ExpError} -> {Input, ExpError};
            {different, Input, ExpOutput} -> {Input, {ok, ExpOutput}};
            Term -> {Term, {ok, Term}}
        end,

        ?assertEqual(ExpectedExpandResult, ?rpc(atm_value:from_store_item(
            AtmWorkflowExecutionAuth,
            atm_value:to_store_item(InitialItem, AtmDataSpec),
            AtmDataSpec
        )))
    end, Values).


%% @private
atm_value_describe_test_base(#atm_value_describe_testcase{
    data_spec = AtmDataSpec,
    values = Values
}) ->
    AtmWorkflowExecutionAuth = create_workflow_execution_auth(),

    lists:foreach(fun(Value) ->
        {InitialItem, ExpectedExpandResult} = case Value of
            {error, Input, ExpError} -> {Input, ExpError};
            {different, Input, ExpOutput} -> {Input, {ok, ExpOutput}};
            Term -> {Term, {ok, Term}}
        end,

        ?assertEqual(ExpectedExpandResult, ?rpc(atm_value:describe_store_item(
            AtmWorkflowExecutionAuth,
            atm_value:to_store_item(InitialItem, AtmDataSpec),
            AtmDataSpec
        )))
    end, Values).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec create_workflow_execution_auth() -> atm_workflow_execution_auth:record().
create_workflow_execution_auth() ->
    atm_store_test_utils:create_workflow_execution_auth(
        ?PROVIDER_SELECTOR, user2, space_krk
    ).


%% @private
-spec build_rand_ts_measurement() -> json_utils:json_map().
build_rand_ts_measurement() ->
    build_rand_ts_measurement(?RAND_STR()).


%% @private
-spec build_rand_ts_measurement(atm_time_series_names:measurement_ts_name()) ->
    json_utils:json_map().
build_rand_ts_measurement(TSName) ->
    #{
        <<"tsName">> => TSName,
        <<"timestamp">> => ?RAND_INT(10000),
        <<"value">> => ?RAND_ELEMENT([1, -1]) * ?RAND_INT(10000)
    }.


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
