const db = require('./db')
const gerador = require('./gerador')

const cadastro = async (entidade, body) => {
    const newTuplas = Object.keys(body).length > 0 ? [body] : await db.getNewTuplas(entidade);
    switch (entidade) {
        case 'cliente':
            for (let newTupla of newTuplas) {
                const idCadastro = await db.insereCliente(newTupla);
                if (idCadastro) return idCadastro
            }
            return undefined;
        case 'produto':
            for (let newTupla of newTuplas) {
                const idCadastro = await db.insereProduto(newTupla);
                if (idCadastro) return idCadastro
            }
            return undefined;
        case 'vendedor':
            for (let newTupla of newTuplas) {
                const idCadastro = await db.insereVendedor(newTupla);
                if (idCadastro) return idCadastro
            }
            return undefined;
        case 'venda':
            const idCadastro = await gerador.venda()
            return idCadastro
        default:
            return undefined
    }
    return newTuplas
}

module.exports = {
    cadastro
}