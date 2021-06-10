<template>
  <el-table
    v-loading="listLoading"
    :data="unusualDiseaseInfo"
    class="boxCard-container"
    highlight-current-row
    v-show="showTableSign"
    ref="multipleTable"
  >
    <el-table-column label="姓名" width="150px">
      <template slot-scope="{row}">
        <span> {{ row.Name }} </span>
      </template>
    </el-table-column>
    <el-table-column label="住院编码">
      <template slot-scope="{row}">
        <span> {{ row.HosRegisterCode }} </span>
      </template>
    </el-table-column>
    <el-table-column label="性别" width="100px">
      <template slot-scope="{row}">
        <span> {{ row.Sex }} </span>
      </template>
    </el-table-column>
    <el-table-column label="年龄" width="100px">
      <template slot-scope="{row}">
        <span> {{ row.Age }} </span>
      </template>
    </el-table-column>
    <el-table-column label="费用" width="150px">
      <template slot-scope="{row}">
        <span> {{ row.TotalFee +"元" }} </span>
      </template>
    </el-table-column>
  </el-table>
</template>

<script>

export default {

  filters: {
    statusFilter(status) {
      const statusMap = {
        success: 'success',
        pending: 'danger'
      }
      return statusMap[status]
    }
  },
  data() {
    return {
      tableColumnValue: null,
      showTableSign: false,
      listLoading: false,
      unusualDiseaseInfo: [],
      boxChartQuery: {
        diseaseCodeKey: "",
        diseaseNameKey: ""
      }
    }
  },
  mounted() {
    this.bus.$on('sendUnusualDiseaseInfo',  (data) => {
      this.unusualDiseaseInfo = data;
      this.initChart();
    })
    this.bus.$on("changeTableValue", (data) => {
      console.log('click_change')
      console.log(data)
      // this.tableColumnValue = data
      let values = this.unusualDiseaseInfo;
      this.unusualDiseaseInfo = [data].concat(values.filter(item => item !== data));
      //从后台拿到数据直接取this.privateData[0]第一条
      this.$refs.multipleTable.setCurrentRow(this.unusualDiseaseInfo[0],true)
    })
  },
  methods: {
    initChart() {
      this.showTableSign = true
    }
  }
}
</script>

<style>
.boxCard-container {
  width: 100%;
  height: 100%;
  overflow: scroll;
}
</style>
