from __future__ import absolute_import
from __future__ import unicode_literals

import os
import sys

import webapp2
from google.appengine.ext import ndb

cwd = os.getcwd()
sys.path.insert(0, 'includes')
from datavalidation import DataValidation
from GCP_return_codes import FunctionReturnCodes as RC
from task_queue_functions import TaskQueueFunctions
from p1_services import Services, TaskArguments
from p1_global_settings import PostDataRules
from p1_datastores import Datastores
from datastore_functions import DatastoreFunctions as DSF


class CommonPostHandler(DataValidation):
    def post(self):
        task_id = "modify-joins:CommonPostHandler:post"
        debug_data = []
        call_result = self.processPushTask()
        debug_data.append(call_result)
        task_results = call_result['task_results']

        params = {}
        for key in self.request.arguments():
            params[key] = self.request.get(key, None)
        task_functions = TaskQueueFunctions()

        if call_result['success'] != RC.success:
            task_functions.logError(
                call_result['success'], task_id, params,
                self.request.get('X-AppEngine-TaskName', None),
                self.request.get('transaction_id', None), call_result['return_msg'], debug_data,
                self.request.get('transaction_user_uid', None)
            )
            task_functions.logTransactionFailed(self.request.get('transaction_id', None), call_result['success'])
            if call_result['success'] < RC.retry_threshold:
                self.response.set_status(500)
            else:
                #any other failure scenario will continue to fail no matter how many times its called.
                self.response.set_status(200)
            return

        # go to the next function
        task_functions = TaskQueueFunctions()
        call_result = task_functions.nextTask(task_id, task_results, params)
        debug_data.append(call_result)
        if call_result['success'] != RC.success:
            task_functions.logError(
                call_result['success'], task_id, params,
                self.request.get('X-AppEngine-TaskName', None),
                self.request.get('transaction_id', None), call_result['return_msg'], debug_data,
                self.request.get('transaction_user_uid', None)
            )
        # </end> go to the next function
        self.response.set_status(200)


class AddModifyClusterUser(webapp2.RequestHandler, CommonPostHandler):
    def processPushTask(self):
        task_id = "modify-joins:AddModifyClusterUser:processPushTask"
        return_msg = task_id + ": "
        debug_data = []
        task_results = {}

        # verify input data
        transaction_id = unicode(self.request.get("transaction_id", ""))
        transaction_user_uid = unicode(self.request.get("transaction_user_uid", ""))
        cluster_uid = unicode(self.request.get(TaskArguments.s2t1_cluster_uid, ""))
        user_uid = unicode(self.request.get(TaskArguments.s2t1_user_uid, ""))
        user_roles = unicode(self.request.get(TaskArguments.s2t1_user_roles, ""))

        call_result = self.ruleCheck([
            [transaction_id, PostDataRules.required_name],
            [transaction_user_uid, PostDataRules.internal_uid],
            [cluster_uid, PostDataRules.internal_uid],
            [user_uid, PostDataRules.internal_uid],
            [user_roles, PostDataRules.required_name],
        ])
        debug_data.append(call_result)
        if call_result['success'] != RC.success:
            return_msg += "input validation failed"
            return {
                'success': RC.input_validation_failed, 'return_msg': return_msg, 'debug_data': debug_data,
                'task_results': task_results,
            }

        cluster_uid = long(cluster_uid)
        user_uid = long(user_uid)

        existings_keys = [
            ndb.Key(Datastores.cluster._get_kind(), cluster_uid),
            ndb.Key(Datastores.users._get_kind(), user_uid),
        ]

        for existing_key in existings_keys:
            call_result = DSF.kget(existing_key)
            debug_data.append(call_result)
            if call_result['success'] != RC.success:
                return_msg += "Datastore access failed"
                return {
                    'success': RC.datastore_failure, 'return_msg': return_msg, 'debug_data': debug_data,
                    'task_results': task_results,
                }
            if not call_result['get_result']:
                return_msg += "{} not found".format(existing_key.kind())
                return {
                    'success': RC.input_validation_failed, 'return_msg': return_msg, 'debug_data': debug_data,
                    'task_results': task_results,
                }
        # </end> verify input data

        parent_key = ndb.Key(Datastores.cluster._get_kind(), cluster_uid)
        key_name = "{}|{}".format(user_uid, cluster_uid)
        joins = Datastores.cluster_joins(id=key_name, parent=parent_key)
        joins.user_uid = unicode(user_uid)
        joins.cluster_uid = unicode(cluster_uid)
        joins.roles = user_roles
        call_result = joins.kput()
        debug_data.append(call_result)
        if call_result['success'] != RC.success:
            return_msg += "failed to write needer_need_joins to datastore"
            return {
                'success': call_result['success'], 'return_msg': return_msg, 'debug_data': debug_data,
                'task_results': task_results
            }

        task_results['uid'] = call_result['put_result'].id()

        return {'success': RC.success, 'return_msg': return_msg, 'debug_data': debug_data, 'task_results': task_results}


class RemoveUserFromCluster(webapp2.RequestHandler, CommonPostHandler):
    def processPushTask(self):
        task_id = "modify-joins:RemoveUserFromCluster:processPushTask"
        return_msg = task_id + ": "
        debug_data = []
        task_results = {}

        # verify input data
        transaction_id = unicode(self.request.get("transaction_id", ""))
        transaction_user_uid = unicode(self.request.get("transaction_user_uid", ""))
        cluster_uid = unicode(self.request.get(TaskArguments.s2t2_cluster_uid, ""))
        user_uid = unicode(self.request.get(TaskArguments.s2t2_user_uid, ""))

        call_result = self.ruleCheck([
            [transaction_id, PostDataRules.required_name],
            [transaction_user_uid, PostDataRules.internal_uid],
            [cluster_uid, PostDataRules.internal_uid],
            [user_uid, PostDataRules.internal_uid],
        ])
        debug_data.append(call_result)
        if call_result['success'] != RC.success:
            return_msg += "input validation failed"
            return {
                'success': RC.input_validation_failed, 'return_msg': return_msg, 'debug_data': debug_data,
                'task_results': task_results,
            }

        transaction_user_uid = long(transaction_user_uid)
        cluster_uid = long(cluster_uid)
        user_uid = long(user_uid)

        existings_keys = [
            ndb.Key(Datastores.cluster._get_kind(), cluster_uid),
            ndb.Key(Datastores.users._get_kind(), user_uid),
        ]

        for existing_key in existings_keys:
            call_result = DSF.kget(existing_key)
            debug_data.append(call_result)
            if call_result['success'] != RC.success:
                return_msg += "Datastore access failed"
                return {
                    'success': RC.datastore_failure, 'return_msg': return_msg, 'debug_data': debug_data,
                    'task_results': task_results,
                }
            if not call_result['get_result']:
                return_msg += "{} not found".format(existing_key.kind())
                return {
                    'success': RC.input_validation_failed, 'return_msg': return_msg, 'debug_data': debug_data,
                    'task_results': task_results,
                }
        # </end> verify input data

        key = ndb.Key(
            Datastores.cluster._get_kind(), cluster_uid,
            Datastores.cluster_joins._get_kind(), "{}|{}".format(user_uid, cluster_uid)
        )

        call_result = DSF.kget(key)
        debug_data.append(call_result)
        if call_result['success'] != RC.success:
            return_msg += "failed to load cluster_joins from datastore"
            return {
                'success': call_result['success'], 'return_msg': return_msg, 'debug_data': debug_data,
                'task_results': task_results
            }
        cluster_join = call_result['get_result']
        if not cluster_join:
            return {'success': RC.success, 'return_msg': return_msg, 'debug_data': debug_data, 'task_results': task_results}

        cluster_join.replicateEntityToFirebase(delete_flag=True)

        call_result = DSF.kdelete(transaction_user_uid, key)
        debug_data.append(call_result)
        if call_result['success'] != RC.success:
            return_msg += "failed to delete cluster_joins from datastore"
            return {
                'success': call_result['success'], 'return_msg': return_msg, 'debug_data': debug_data,
                'task_results': task_results
            }

        return {'success': RC.success, 'return_msg': return_msg, 'debug_data': debug_data, 'task_results': task_results}


class AddModifyUserSkill(webapp2.RequestHandler, CommonPostHandler):
    def processPushTask(self):
        task_id = "modify-joins:AddModifyUserSkill:processPushTask"
        return_msg = task_id + ": "
        debug_data = []
        task_results = {}

        # verify input data
        transaction_id = unicode(self.request.get("transaction_id", ""))
        transaction_user_uid = unicode(self.request.get("transaction_user_uid", ""))
        user_uid = unicode(self.request.get(TaskArguments.s2t3_user_uid, ""))
        skill_uid = unicode(self.request.get(TaskArguments.s2t3_skill_uid, ""))
        special_notes = unicode(self.request.get(TaskArguments.s2t3_special_notes, "")) or None

        call_result = self.ruleCheck([
            [transaction_id, PostDataRules.required_name],
            [transaction_user_uid, PostDataRules.internal_uid],
            [user_uid, PostDataRules.internal_uid],
            [skill_uid, PostDataRules.internal_uid],
            [special_notes, Datastores.caretaker_skills_joins._rule_special_notes],
        ])
        debug_data.append(call_result)
        if call_result['success'] != RC.success:
            return_msg += "input validation failed"
            return {
                'success': RC.input_validation_failed, 'return_msg': return_msg, 'debug_data': debug_data,
                'task_results': task_results,
            }

        user_uid = long(user_uid)
        skill_uid = long(skill_uid)

        existings_keys = [
            ndb.Key(Datastores.users._get_kind(), user_uid),
            ndb.Key(Datastores.caretaker_skills._get_kind(), skill_uid),
        ]

        for existing_key in existings_keys:
            call_result = DSF.kget(existing_key)
            debug_data.append(call_result)
            if call_result['success'] != RC.success:
                return_msg += "Datastore access failed"
                return {
                    'success': RC.datastore_failure, 'return_msg': return_msg, 'debug_data': debug_data,
                    'task_results': task_results,
                }
            if not call_result['get_result']:
                return_msg += "{} not found".format(existing_key.kind())
                return {
                    'success': RC.input_validation_failed, 'return_msg': return_msg, 'debug_data': debug_data,
                    'task_results': task_results,
                }
        # </end> verify input data

        parent_key = ndb.Key(Datastores.cluster._get_kind(), user_uid)
        key_name = "{}|{}".format(user_uid, skill_uid)
        joins = Datastores.caretaker_skills_joins(id=key_name, parent=parent_key)
        joins.user_uid = user_uid
        joins.skill_uid = skill_uid
        joins.special_notes = special_notes
        call_result = joins.kput()
        debug_data.append(call_result)
        if call_result['success'] != RC.success:
            return_msg += "failed to write caretaker_skills_joins to datastore"
            return {
                'success': call_result['success'], 'return_msg': return_msg, 'debug_data': debug_data,
                'task_results': task_results
            }

        task_results['uid'] = call_result['put_result'].id()

        return {'success': RC.success, 'return_msg': return_msg, 'debug_data': debug_data, 'task_results': task_results}


class AddModifyNeedToNeeder(webapp2.RequestHandler, CommonPostHandler):
    def processPushTask(self):
        task_id = "modify-joins:AddModifyNeedToNeeder:processPushTask"
        return_msg = task_id + ": "
        debug_data = []
        task_results = {}

        # verify input data
        transaction_id = unicode(self.request.get("transaction_id", ""))
        transaction_user_uid = unicode(self.request.get("transaction_user_uid", ""))
        need_uid = unicode(self.request.get(TaskArguments.s2t4_need_uid, ""))
        needer_uid = unicode(self.request.get(TaskArguments.s2t4_needer_uid, ""))
        user_uid = unicode(self.request.get(TaskArguments.s2t4_user_uid, ""))
        special_requirements = unicode(self.request.get(TaskArguments.s2t4_special_requests, "")) or None

        call_result = self.ruleCheck([
            [transaction_id, PostDataRules.required_name],
            [transaction_user_uid, PostDataRules.internal_uid],
            [need_uid, Datastores.needer_needs_joins._rule_need_uid],
            [needer_uid, Datastores.needer_needs_joins._rule_needer_uid],
            [user_uid, Datastores.needer_needs_joins._rule_user_uid],
            [special_requirements, Datastores.needer_needs_joins._rule_special_requests],
        ])
        debug_data.append(call_result)
        if call_result['success'] != RC.success:
            return_msg += "input validation failed"
            return {
                'success': RC.input_validation_failed, 'return_msg': return_msg, 'debug_data': debug_data,
                'task_results': task_results,
            }

        needer_uid = long(needer_uid)
        need_uid = long(need_uid)
        user_uid = long(user_uid)

        existings_keys = [
            ndb.Key(Datastores.users._get_kind(), user_uid),
            ndb.Key(Datastores.users._get_kind(), user_uid, Datastores.needer._get_kind(), needer_uid),
            ndb.Key(Datastores.needs._get_kind(), need_uid),
        ]

        for existing_key in existings_keys:
            call_result = DSF.kget(existing_key)
            debug_data.append(call_result)
            if call_result['success'] != RC.success:
                return_msg += "Datastore access failed"
                return {
                    'success': RC.datastore_failure, 'return_msg': return_msg, 'debug_data': debug_data,
                    'task_results': task_results,
                }
            if not call_result['get_result']:
                return_msg += "{} not found".format(existing_key.kind())
                return {
                    'success': RC.input_validation_failed, 'return_msg': return_msg, 'debug_data': debug_data,
                    'task_results': task_results,
                }
        # </end> verify input data

        parent_key = ndb.Key(Datastores.users._get_kind(), user_uid, Datastores.needer._get_kind(), needer_uid)
        key_name = "{}|{}".format(needer_uid, need_uid)
        joins = Datastores.needer_needs_joins(id=key_name, parent=parent_key)
        joins.needer_uid = unicode(needer_uid)
        joins.need_uid = unicode(need_uid)
        joins.user_uid = unicode(user_uid)
        joins.special_requests = special_requirements
        call_result = joins.kput()
        debug_data.append(call_result)
        if call_result['success'] != RC.success:
            return_msg += "failed to write needer_need_joins to datastore"
            return {
                'success': call_result['success'], 'return_msg': return_msg, 'debug_data': debug_data,
                'task_results': task_results
            }

        task_results['uid'] = call_result['put_result'].id()

        return {'success': RC.success, 'return_msg': return_msg, 'debug_data': debug_data, 'task_results': task_results}


class RemoveNeedFromNeeder(webapp2.RequestHandler, CommonPostHandler):
    def processPushTask(self):
        task_id = "modify-joins:RemoveNeedFromNeeder:processPushTask"
        return_msg = task_id + ": "
        debug_data = []
        task_results = {}

        # verify input data
        transaction_id = unicode(self.request.get("transaction_id", ""))
        transaction_user_uid = unicode(self.request.get("transaction_user_uid", ""))
        need_uid = unicode(self.request.get(TaskArguments.s2t5_need_uid, ""))
        needer_uid = unicode(self.request.get(TaskArguments.s2t5_needer_uid, ""))
        user_uid = unicode(self.request.get(TaskArguments.s2t5_user_uid, ""))

        call_result = self.ruleCheck([
            [transaction_id, PostDataRules.required_name],
            [transaction_user_uid, PostDataRules.internal_uid],
            [need_uid, PostDataRules.internal_uid],
            [needer_uid, PostDataRules.internal_uid],
            [user_uid, PostDataRules.internal_uid],
        ])
        debug_data.append(call_result)
        if call_result['success'] != RC.success:
            return_msg += "input validation failed"
            return {
                'success': RC.input_validation_failed, 'return_msg': return_msg, 'debug_data': debug_data,
                'task_results': task_results,
            }

        transaction_user_uid = long(transaction_user_uid)
        needer_uid = long(needer_uid)
        need_uid = long(need_uid)
        user_uid = long(user_uid)

        existings_keys = [
            ndb.Key(Datastores.users._get_kind(), user_uid),
            ndb.Key(Datastores.users._get_kind(), user_uid, Datastores.needer._get_kind(), needer_uid),
            ndb.Key(Datastores.needs._get_kind(), need_uid),
        ]

        for existing_key in existings_keys:
            call_result = DSF.kget(existing_key)
            debug_data.append(call_result)
            if call_result['success'] != RC.success:
                return_msg += "Datastore access failed"
                return {
                    'success': RC.datastore_failure, 'return_msg': return_msg, 'debug_data': debug_data,
                    'task_results': task_results,
                }
            if not call_result['get_result']:
                return_msg += "{} not found".format(existing_key.kind())
                return {
                    'success': RC.input_validation_failed, 'return_msg': return_msg, 'debug_data': debug_data,
                    'task_results': task_results,
                }
        # </end> verify input data

        key = ndb.Key(
            Datastores.users._get_kind(), user_uid,
            Datastores.needer._get_kind(), needer_uid,
            Datastores.needer_needs_joins._get_kind(), "{}|{}".format(needer_uid, need_uid)
        )
        call_result = DSF.kdelete(transaction_user_uid, key)
        debug_data.append(call_result)
        if call_result['success'] != RC.success:
            return_msg += "failed to delete needer_need_joins from datastore"
            return {
                'success': call_result['success'], 'return_msg': return_msg, 'debug_data': debug_data,
                'task_results': task_results
            }

        return {'success': RC.success, 'return_msg': return_msg, 'debug_data': debug_data, 'task_results': task_results}


class RemoveNeederFromUser(webapp2.RequestHandler, CommonPostHandler):
    def processPushTask(self):
        task_id = "modify-joins:RemoveNeederFromUser:processPushTask"
        return_msg = task_id + ": "
        debug_data = []
        task_results = {}

        # verify input data
        transaction_id = unicode(self.request.get("transaction_id", ""))
        transaction_user_uid = unicode(self.request.get("transaction_user_uid", ""))
        needer_uid = unicode(self.request.get(TaskArguments.s2t6_needer_uid, ""))
        user_uid = unicode(self.request.get(TaskArguments.s2t6_user_uid, ""))

        call_result = self.ruleCheck([
            [transaction_id, PostDataRules.required_name],
            [transaction_user_uid, PostDataRules.internal_uid],
            [needer_uid, PostDataRules.internal_uid],
            [user_uid, PostDataRules.internal_uid],
        ])
        debug_data.append(call_result)
        if call_result['success'] != RC.success:
            return_msg += "input validation failed"
            return {
                'success': RC.input_validation_failed, 'return_msg': return_msg, 'debug_data': debug_data,
                'task_results': task_results,
            }

        transaction_user_uid = long(transaction_user_uid)
        needer_uid = long(needer_uid)
        user_uid = long(user_uid)

        existings_keys = [
            ndb.Key(Datastores.users._get_kind(), user_uid),
            ndb.Key(Datastores.users._get_kind(), user_uid, Datastores.needer._get_kind(), needer_uid),
        ]

        for existing_key in existings_keys:
            call_result = DSF.kget(existing_key)
            debug_data.append(call_result)
            if call_result['success'] != RC.success:
                return_msg += "Datastore access failed"
                return {
                    'success': RC.datastore_failure, 'return_msg': return_msg, 'debug_data': debug_data,
                    'task_results': task_results,
                }
            if not call_result['get_result']:
                return_msg += "{} not found".format(existing_key.kind())
                return {
                    'success': RC.input_validation_failed, 'return_msg': return_msg, 'debug_data': debug_data,
                    'task_results': task_results,
                }
        # </end> verify input data

        key = ndb.Key(
            Datastores.users._get_kind(), user_uid,
            Datastores.needer._get_kind(), needer_uid
        )
        call_result = DSF.kdelete(transaction_user_uid, key)
        debug_data.append(call_result)
        if call_result['success'] != RC.success:
            return_msg += "failed to delete needer from datastore"
            return {
                'success': call_result['success'], 'return_msg': return_msg, 'debug_data': debug_data,
                'task_results': task_results
            }

        return {'success': RC.success, 'return_msg': return_msg, 'debug_data': debug_data, 'task_results': task_results}


class AssignHashtagToUser(webapp2.RequestHandler, CommonPostHandler):
    def processPushTask(self):
        task_id = "modify-joins:AssignHashtagToUser:processPushTask"
        return_msg = task_id + ": "
        debug_data = []
        task_results = {}

        # verify input data
        transaction_id = unicode(self.request.get("transaction_id", ""))
        transaction_user_uid = unicode(self.request.get("transaction_user_uid", ""))
        user_uid = unicode(self.request.get(TaskArguments.s2t7_user_uid, ""))
        hashtag_uid = unicode(self.request.get(TaskArguments.s2t7_hashtag_uid, ""))

        call_result = self.ruleCheck([
            [transaction_id, PostDataRules.required_name],
            [transaction_user_uid, PostDataRules.internal_uid],
            [user_uid, PostDataRules.internal_uid],
            [hashtag_uid, PostDataRules.internal_uid],
        ])
        debug_data.append(call_result)
        if call_result['success'] != RC.success:
            return_msg += "input validation failed"
            return {
                'success': RC.input_validation_failed, 'return_msg': return_msg, 'debug_data': debug_data,
                'task_results': task_results,
            }

        user_uid = long(user_uid)
        hashtag_uid = long(hashtag_uid)

        existings_keys = [
            ndb.Key(Datastores.users._get_kind(), user_uid),
            ndb.Key(Datastores.hashtags._get_kind(), hashtag_uid),
        ]

        for existing_key in existings_keys:
            call_result = DSF.kget(existing_key)
            debug_data.append(call_result)
            if call_result['success'] != RC.success:
                return_msg += "Datastore access failed"
                return {
                    'success': RC.datastore_failure, 'return_msg': return_msg, 'debug_data': debug_data,
                    'task_results': task_results,
                }
            if not call_result['get_result']:
                return_msg += "{} not found".format(existing_key.kind())
                return {
                    'success': RC.input_validation_failed, 'return_msg': return_msg, 'debug_data': debug_data,
                    'task_results': task_results,
                }
        # </end> verify input data

        parent_key = ndb.Key(Datastores.users._get_kind(), user_uid)
        pointer = Datastores.hashtag_pointer(id=hashtag_uid, parent=parent_key)
        pointer.hashtag_uid = hashtag_uid
        call_result = pointer.kput()
        debug_data.append(call_result)
        if call_result['success'] != RC.success:
            return_msg += "failed to write hashtag_pointer to datastore"
            return {
                'success': call_result['success'], 'return_msg': return_msg, 'debug_data': debug_data,
                'task_results': task_results
            }

        task_results['uid'] = call_result['put_result'].id()

        return {'success': RC.success, 'return_msg': return_msg, 'debug_data': debug_data, 'task_results': task_results}


class RemoveHashtagFromUser(webapp2.RequestHandler, CommonPostHandler):
    def processPushTask(self):
        task_id = "modify-joins:RemoveHashtagFromUser:processPushTask"
        return_msg = task_id + ": "
        debug_data = []
        task_results = {}

        # verify input data
        transaction_id = unicode(self.request.get("transaction_id", ""))
        transaction_user_uid = unicode(self.request.get("transaction_user_uid", ""))
        user_uid = unicode(self.request.get(TaskArguments.s2t8_user_uid, ""))
        hashtag_uid = unicode(self.request.get(TaskArguments.s2t8_hashtag_uid, ""))

        call_result = self.ruleCheck([
            [transaction_id, PostDataRules.required_name],
            [transaction_user_uid, PostDataRules.internal_uid],
            [user_uid, PostDataRules.internal_uid],
            [hashtag_uid, PostDataRules.internal_uid],
        ])
        debug_data.append(call_result)
        if call_result['success'] != RC.success:
            return_msg += "input validation failed"
            return {
                'success': RC.input_validation_failed, 'return_msg': return_msg, 'debug_data': debug_data,
                'task_results': task_results,
            }

        transaction_user_uid = long(transaction_user_uid)
        user_uid = long(user_uid)
        hashtag_uid = long(hashtag_uid)

        existings_keys = [
            ndb.Key(Datastores.users._get_kind(), user_uid),
            ndb.Key(Datastores.hashtags._get_kind(), hashtag_uid),
        ]

        for existing_key in existings_keys:
            call_result = DSF.kget(existing_key)
            debug_data.append(call_result)
            if call_result['success'] != RC.success:
                return_msg += "Datastore access failed"
                return {
                    'success': RC.datastore_failure, 'return_msg': return_msg, 'debug_data': debug_data,
                    'task_results': task_results,
                }
            if not call_result['get_result']:
                return_msg += "{} not found".format(existing_key.kind())
                return {
                    'success': RC.input_validation_failed, 'return_msg': return_msg, 'debug_data': debug_data,
                    'task_results': task_results,
                }
        # </end> verify input data

        key = ndb.Key(Datastores.users._get_kind(), user_uid, Datastores.hashtag_pointer._get_kind(), hashtag_uid)
        call_result = DSF.kdelete(transaction_user_uid, key)
        debug_data.append(call_result)
        if call_result['success'] != RC.success:
            return_msg += "failed to delete hashtag_pointer from datastore"
            return {
                'success': call_result['success'], 'return_msg': return_msg, 'debug_data': debug_data,
                'task_results': task_results
            }

        return {'success': RC.success, 'return_msg': return_msg, 'debug_data': debug_data, 'task_results': task_results}


class RemoveSkillFromUser(webapp2.RequestHandler, CommonPostHandler):
    def processPushTask(self):
        task_id = "modify-joins:RemoveSkillFromUser:processPushTask"
        return_msg = task_id + ": "
        debug_data = []
        task_results = {}

        # verify input data
        transaction_id = unicode(self.request.get("transaction_id", ""))
        transaction_user_uid = unicode(self.request.get("transaction_user_uid", ""))
        user_uid = unicode(self.request.get(TaskArguments.s2t9_user_uid, ""))
        skill_uid = unicode(self.request.get(TaskArguments.s2t9_skill_uid, ""))

        call_result = self.ruleCheck([
            [transaction_id, PostDataRules.required_name],
            [transaction_user_uid, PostDataRules.internal_uid],
            [user_uid, PostDataRules.internal_uid],
            [skill_uid, PostDataRules.internal_uid],
        ])
        debug_data.append(call_result)
        if call_result['success'] != RC.success:
            return_msg += "input validation failed"
            return {
                'success': RC.input_validation_failed, 'return_msg': return_msg, 'debug_data': debug_data,
                'task_results': task_results,
            }

        transaction_user_uid = long(transaction_user_uid)
        user_uid = long(user_uid)
        skill_uid = long(skill_uid)

        existings_keys = [
            ndb.Key(Datastores.users._get_kind(), user_uid),
            ndb.Key(Datastores.caretaker_skills._get_kind(), skill_uid),
        ]

        for existing_key in existings_keys:
            call_result = DSF.kget(existing_key)
            debug_data.append(call_result)
            if call_result['success'] != RC.success:
                return_msg += "Datastore access failed"
                return {
                    'success': RC.datastore_failure, 'return_msg': return_msg, 'debug_data': debug_data,
                    'task_results': task_results,
                }
            if not call_result['get_result']:
                return_msg += "{} not found".format(existing_key.kind())
                return {
                    'success': RC.input_validation_failed, 'return_msg': return_msg, 'debug_data': debug_data,
                    'task_results': task_results,
                }
        # </end> verify input data

        key = ndb.Key(
            Datastores.users._get_kind(), user_uid,
            Datastores.caretaker_skills_joins._get_kind(), "{}|{}".format(user_uid, skill_uid)
        )
        call_result = DSF.kdelete(transaction_user_uid, key)
        debug_data.append(call_result)
        if call_result['success'] != RC.success:
            return_msg += "failed to delete caretaker_skills_joins from datastore"
            return {
                'success': call_result['success'], 'return_msg': return_msg, 'debug_data': debug_data,
                'task_results': task_results
            }

        return {'success': RC.success, 'return_msg': return_msg, 'debug_data': debug_data, 'task_results': task_results}


app = webapp2.WSGIApplication([
    (Services.modify_joins.add_modify_cluster_user.url, AddModifyClusterUser),
    (Services.modify_joins.remove_user_from_cluster.url, RemoveUserFromCluster),
    (Services.modify_joins.add_modify_user_skill.url, AddModifyUserSkill),
    (Services.modify_joins.add_modify_need_to_needer.url, AddModifyNeedToNeeder),
    (Services.modify_joins.remove_need_from_needer.url, RemoveNeedFromNeeder),
    (Services.modify_joins.remove_needer_from_user.url, RemoveNeederFromUser),
    (Services.modify_joins.assign_hashtag_to_user.url, AssignHashtagToUser),
    (Services.modify_joins.remove_hashtag_from_user.url, RemoveHashtagFromUser),
    (Services.modify_joins.remove_skill_from_user.url, RemoveSkillFromUser),
], debug=True)
