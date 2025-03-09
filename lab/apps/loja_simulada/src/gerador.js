const db = require('./db');
const kafka = require('./kafka');

/* COLOCAR NO BANCO */
let vendasPorMinuto = 2;
let loopInterval = null;

const getRandomInt = (min, max) => {
    const result = Math.floor(Math.random() * (max - min) + min);
    // return Math.floor(Math.random() * (Math.floor(max) - 1) + 1);
    return result;
}

const getValidRandomInt = (numeros, somenteValidos = false) => {
    const min = numeros[0];
    const max = numeros[numeros.length - 1];
    let num;
    do {
        num = getRandomInt(min, max);
    } while (somenteValidos && !numeros.includes(num));
    return num;
}


const getDate = () => {
    const agora = new Date();
    const ano = agora.getFullYear();
    const mes = agora.getMonth() < 10 ? `0${agora.getMonth()}` : agora.getMonth();
    const dia = agora.getDate() < 10 ? `0${agora.getDate()}` : agora.getDate();
    return `${ano}-${mes}-${dia}`;
}

const getIds = (tuplas) => {
    const ids = [];
    tuplas.forEach(element =>  ids.push(Object.values(element)[0]));
    return ids;
}

const getValor = (produtos, idProduto) => {
    return produtos.filter(el => el.id_produto == idProduto)[0].preco;
}

const venda = async () => {
    const idvendedores = await db.getIdVendedores();
    const idClientes = await db.getIdClientes();
    const produtos = await db.getProdutos();
    const idsProdutos = getIds(produtos);
    const idProduto = getValidRandomInt(idsProdutos, true);
    const quantidade = getValidRandomInt([1,9]);
    const precoUnitario = Number(getValor(produtos, idProduto));
    const precoTotal = Number((precoUnitario * quantidade).toFixed(2));
    const venda = {
        id_vendedor: getValidRandomInt(idvendedores, true),
        id_cliente: getValidRandomInt(idClientes, true),
        data: getDate(),
        total: precoTotal,
    };
    const idVenda = await db.insereVenda(venda);
    const itemVenda = {
        id_produto: idProduto,
        id_venda: idVenda,
        quantidade: quantidade,
        valor_unitario: precoUnitario,
        valor_total: precoTotal,
        desconto: Number(((getValidRandomInt([1,10]) / 100) * precoTotal).toFixed(2)),
    }
    const idItemVenda = await db.insereItemVenda(itemVenda);
    // kafka.Publicar([
    //     JSON.stringify({
    //         id_vendedor: venda.id_vendedor,
    //         id_cliente: venda.id_cliente,
    //         id_produto: itemVenda.id_produto,
    //         id_venda: itemVenda.id_venda,
    //         quantidade: itemVenda.quantidade,
    //         valor_unitario: itemVenda.valor_unitario,
    //         valor_total: itemVenda.valor_total,
    //         desconto: itemVenda.desconto,
    //         data: venda.data,
    //     }),
    // ]);
    return {
        venda: idVenda,
        itemVenda: idItemVenda,
    };
}

const autoGenVendas = (newValue = 3) => {
    clearInterval(loopInterval)
    loopInterval = setInterval(() => {
        venda();
        console.log('venda realizada!')
    }, (60000 / newValue));
}
autoGenVendas()

module.exports = {
    venda,
    autoGenVendas
}