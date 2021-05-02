<template>
  <div class="app-container">
    <div class="filter-container">
      <el-select v-model="listQuery.keyWord" style="width: 110px" class="filter-item">
        <el-option v-for="item in keyWordOptions" :key="item.key" :label="item.label" :value="item.value" />
      </el-select>
      <el-input v-model="listQuery.searchContent" placeholder="检索内容" style="width: 200px;" class="filter-item" @keyup.enter.native="handleFilter" />
      <el-button v-waves class="filter-item" type="primary" icon="el-icon-search" @click="handleFilter">
        搜索
      </el-button>
    </div>

    <el-table
      :key="tableKey"
      v-loading="listLoading"
      :data="list"
      border
      fit
      highlight-current-row
      style="width: 100%;"
      @sort-change="sortChange"
    >
      <el-table-column label="姓名" width="100px" align="center">
        <template slot-scope="{row}">
          <span>{{ row.Name }}</span>
        </template>
      </el-table-column>

      <el-table-column label="年龄" width="100px">
        <template slot-scope="{row}">
          <span>{{ row.Age }}</span>
        </template>
      </el-table-column>

      <el-table-column label="性别" align="center" width="100">
        <template slot-scope="{row}">
          <span>{{ row.Sex }}</span>
        </template>
      </el-table-column>

      <el-table-column label="人员类型" width="200px" align="center">
        <template slot-scope="{row}">
          <span>{{ row.Desc }}</span>
        </template>
      </el-table-column>

      <el-table-column label="身份证号" prop="id" align="center" width="300">
        <template slot-scope="{row}">
          <span>{{ row.CertificateCode }}</span>
        </template>
      </el-table-column>

      <el-table-column label="住院次数" class-name="status-col" width="100px">
        <template slot-scope="{row}">
          <span class="link-type" @click="handleList(row)">{{ row.Times }}</span>
        </template>
      </el-table-column>

      <el-table-column label="地址" align="center" width="250">
        <template slot-scope="{row}">
          <span>{{ row.AllName }}</span>
        </template>
      </el-table-column>
    </el-table>

    <pagination v-show="total>0" :total="total" :page.sync="listQuery.page" :limit.sync="listQuery.limit" @pagination="getList" />

    <el-dialog :title="textMap[dialogStatus]" :visible.sync="dialogFormVisible">
      <el-table :data="temp" border fit highlight-current-row style="width: 100%;">
        <el-table-column label="住院编码" prop="HosRegisterCode" width="180"></el-table-column>
        <el-table-column label="住院时间" prop="InHosDate" width="95"></el-table-column>
        <el-table-column label="出院时间" prop="OutHosDate" width="95"></el-table-column>
        <el-table-column label="住院天数" prop="DayInHos"></el-table-column>
<!--        <el-table-column label="总费用" prop="TotalFee">-->
        <el-table-column label="总费用">
          <template slot-scope="{row}">
            <span class="link-type" @click="getDrugList(row)">{{ row.TotalFee }}</span>
          </template>
        </el-table-column>
      </el-table>
    </el-dialog>

    <el-dialog :visible.sync="dialogPvVisible">
<!--      <el-form ref="dataForm" :rules="rules" :model="temp" label-position="left" label-width="70px" style="width: 400px; margin-left:50px;">-->
<!--        <el-form-item label="Type" prop="type">-->
<!--          <el-select v-model="temp.type" class="filter-item" placeholder="Please select">-->
<!--            <el-option v-for="item in calendarTypeOptions" :key="item.key" :label="item.display_name" :value="item.key" />-->
<!--          </el-select>-->
<!--        </el-form-item>-->
<!--        <el-form-item label="Date" prop="timestamp">-->
<!--          <el-date-picker v-model="temp.timestamp" type="datetime" placeholder="Please pick a date" />-->
<!--        </el-form-item>-->
<!--        <el-form-item label="标题" prop="title">-->
<!--          <el-input v-model="temp.title" />-->
<!--        </el-form-item>-->
<!--        <el-form-item label="Status">-->
<!--          <el-select v-model="temp.status" class="filter-item" placeholder="Please select">-->
<!--            <el-option v-for="item in statusOptions" :key="item" :label="item" :value="item" />-->
<!--          </el-select>-->
<!--        </el-form-item>-->
<!--        <el-form-item label="Imp">-->
<!--          <el-rate v-model="temp.importance" :colors="['#99A9BF', '#F7BA2A', '#FF9900']" :max="3" style="margin-top:8px;" />-->
<!--        </el-form-item>-->
<!--        <el-form-item label="Remark">-->
<!--          <el-input v-model="temp.remark" :autosize="{ minRows: 2, maxRows: 4}" type="textarea" placeholder="Please input" />-->
<!--        </el-form-item>-->
<!--      </el-form>-->
      <div slot="footer" class="dialog-footer">
        <el-button @click="dialogPvVisible = false">
          Cancel
        </el-button>
      </div>
    </el-dialog>

  </div>
</template>

<script>
import { fetchList, fetchPv, createArticle, updateArticle } from '@/api/article'
import waves from '@/directive/waves' // waves directive
import { parseTime } from '@/utils'
import Pagination from '@/components/Pagination' // secondary package based on el-pagination

const calendarTypeOptions = [
  { key: 'CN', display_name: '中国' },
  { key: 'US', display_name: '美国' },
  { key: 'JP', display_name: '日本' },
  { key: 'EU', display_name: '欧洲' }
]

// arr to obj, such as { CN : "China", US : "USA" }
const calendarTypeKeyValue = calendarTypeOptions.reduce((acc, cur) => {
  acc[cur.key] = cur.display_name
  return acc
}, {})

export default {
  name: 'ComplexTable',
  components: { Pagination },
  directives: { waves },
  filters: {
    statusFilter(status) {
      const statusMap = {
        published: 'success',
        draft: 'info',
        deleted: 'danger'
      }
      return statusMap[status]
    },
    typeFilter(type) {
      return calendarTypeKeyValue[type]
    }
  },
  data() {
    return {
      tableKey: 0,
      list: null,
      total: 0,
      listLoading: false,
      listQuery: {
        page: 1,
        limit: 20,
        keyWord: '',
        searchContent: undefined,
        type: undefined,
        sort: '+id'
      },
      keyWordOptions: [{
        value: 'name',
        label: '姓名'
      },{
        value: 'id',
        label: '身份证号'
      },{
        value: 'hosid',
        label: '住院编码'
      }],
      calendarTypeOptions,
      sortOptions: [{ label: 'ID Ascending', key: '+id' }, { label: 'ID Descending', key: '-id' }],
      statusOptions: ['published', 'draft', 'deleted'],
      showReviewer: false,
      temp: [],
      dialogFormVisible: false,
      dialogStatus: '',
      textMap: {
        update: '',
        create: 'Create'
      },
      dialogPvVisible: false,
      pvData: [],
      rules: {
        type: [{ required: true, message: 'type is required', trigger: 'change' }],
        timestamp: [{ type: 'date', required: true, message: 'timestamp is required', trigger: 'change' }],
        title: [{ required: true, message: 'title is required', trigger: 'blur' }]
      },
      downloadLoading: false
    }
  },
  created() {
    // this.getList()
    this.listQuery.keyWord = this.keyWordOptions[0].value
  },
  methods: {
    getList() {
      this.listLoading = true
      fetchList(this.listQuery).then(response => {
        this.list = response.data.items
        this.total = response.data.total
        this.listLoading = false
      })
    },
    handleFilter() {
      this.listQuery.page = 1
      this.getList()
    },
    getDrugList(drug) {
      this.temp = []
      this.temp = Object.assign({}, drug)
      // 开启dialog
      this.dialogPvVisible = true
    },
    sortChange(data) {
      const { prop, order } = data
      if (prop === 'id') {
        this.sortByID(order)
      }
    },
    sortByID(order) {
      if (order === 'ascending') {
        this.listQuery.sort = '+id'
      } else {
        this.listQuery.sort = '-id'
      }
      this.handleFilter()
    },
    createData() {
      this.$refs['dataForm'].validate((valid) => {
        if (valid) {
          this.temp.id = parseInt(Math.random() * 100) + 1024 // mock a id
          this.temp.author = 'vue-element-admin'
          createArticle(this.temp).then(() => {
            this.list.unshift(this.temp)
            this.dialogFormVisible = false
            this.$notify({
              title: 'Success',
              message: 'Created Successfully',
              type: 'success',
              duration: 2000
            })
          })
        }
      })
    },
    handleList(row) {
      // this.temp = Object.assign({}, row) // copy obj
      console.log(row.Info)
      this.temp = []
      this.temp = row.Info
      console.log(this.temp)
      // this.dialogStatus = 'update'
      this.dialogFormVisible = true
    },
    updateData() {
      this.$refs['dataForm'].validate((valid) => {
        if (valid) {
          const tempData = Object.assign({}, this.temp)
          tempData.timestamp = +new Date(tempData.timestamp) // change Thu Nov 30 2017 16:41:05 GMT+0800 (CST) to 1512031311464
          updateArticle(tempData).then(() => {
            const index = this.list.findIndex(v => v.id === this.temp.id)
            this.list.splice(index, 1, this.temp)
            this.dialogFormVisible = false
            this.$notify({
              title: 'Success',
              message: 'Update Successfully',
              type: 'success',
              duration: 2000
            })
          })
        }
      })
    },
    handleDelete(row, index) {
      this.$notify({
        title: 'Success',
        message: 'Delete Successfully',
        type: 'success',
        duration: 2000
      })
      this.list.splice(index, 1)
    },
    handleDownload() {
      this.downloadLoading = true
      import('@/vendor/Export2Excel').then(excel => {
        const tHeader = ['timestamp', 'title', 'type', 'importance', 'status']
        const filterVal = ['timestamp', 'title', 'type', 'importance', 'status']
        const data = this.formatJson(filterVal)
        excel.export_json_to_excel({
          header: tHeader,
          data,
          filename: 'table-list'
        })
        this.downloadLoading = false
      })
    },
    formatJson(filterVal) {
      return this.list.map(v => filterVal.map(j => {
        if (j === 'timestamp') {
          return parseTime(v[j])
        } else {
          return v[j]
        }
      }))
    }
  }
}
</script>
